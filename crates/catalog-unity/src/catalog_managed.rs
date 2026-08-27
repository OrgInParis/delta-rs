//! Catalog-managed Delta log coordination.
//!
//! Unity Catalog owns transaction visibility for managed tables. This log store delegates data
//! and object I/O to the native delta-rs backend at the catalog-returned location while obtaining
//! the visible version and unbackfilled commit tail exclusively from Unity Catalog.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::kernel::transaction::{PROTOCOL, TransactionError};
use deltalake_core::kernel::{
    Action, CommitInfo, DomainMetadata, Metadata, Protocol, StructType, TableFeatures, Version,
    protocol_with_table_features,
};
use deltalake_core::logstore::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions,
    path::{Error as PathError, Path},
};
use deltalake_core::logstore::{
    CatalogManagedCommit, CatalogManagedState, CatalogManagedStateError, CommitOrBytes,
    CommitPayloadMode, LogStore, LogStoreConfig, LogStoreRef, StorageConfig,
    commit_uri_from_version,
};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::{
    DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError, crate_version, ensure_table_uri,
};
use reqwest::Url;
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

use crate::delta::{
    DeltaClusteringDomainMetadata, DeltaCommit, DeltaCreateTableRequest, DeltaCredentialOperation,
    DeltaCredentialsResponse, DeltaDomainMetadataUpdates, DeltaLoadTableResponse, DeltaProtocol,
    DeltaRowTrackingDomainMetadata, DeltaTableReference, DeltaTableRequirement, DeltaTableType,
    DeltaTableUpdate, DeltaUpdateTableRequest, UNITY_CATALOG_ACCESS_KEY, endpoint,
};
use crate::{UnityCatalog, UnityCatalogBuilder, UnityCatalogConfigKey, UnityCatalogError};

const CATALOG_MANAGED_STORE_NAME: &str = "UnityCatalogManagedLogStore";
const CATALOG_COMMIT_FAILURE: &str = "Unity Catalog managed commit failed";
const STAGED_COMMITS_DIRECTORY: &str = "_delta_log/_staged_commits";
const CLUSTERING_DOMAIN: &str = "delta.clustering";
const ROW_TRACKING_DOMAIN: &str = "delta.rowTracking";
const SYSTEM_DOMAIN_PREFIX: &str = "delta.";

const CALLER_STORAGE_IDENTITY_OPTIONS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_IAM_ROLE_ARN",
    "AWS_IAM_ROLE_SESSION_NAME",
    "AWS_S3_ASSUME_ROLE_ARN",
    "AWS_S3_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_FORCE_CREDENTIAL_LOAD",
    "AWS_ACCESS_KEY_ID_DYNAMODB",
    "AWS_SECRET_ACCESS_KEY_DYNAMODB",
    "AWS_SESSION_TOKEN_DYNAMODB",
];

/// Failures specific to the catalog-managed transaction contract.
#[derive(thiserror::Error, Debug)]
pub enum CatalogManagedError {
    /// Existing-table coordinated commits apply only to managed tables.
    #[error("Unity Catalog returned table type {actual:?}; expected MANAGED")]
    TableType {
        /// Returned table type.
        actual: DeltaTableType,
    },
    /// Managed table responses must identify their latest ratified version.
    #[error("Unity Catalog omitted latest-table-version for a managed table")]
    LatestVersionMissing,
    /// A signed wire version cannot be represented by delta-rs's version type.
    #[error("Unity Catalog returned invalid {field} version {actual}")]
    InvalidVersion {
        /// Wire field containing the version.
        field: &'static str,
        /// Invalid signed value.
        actual: i64,
    },
    /// An internal version exceeds the signed range of the UC wire contract.
    #[error("delta-rs version {actual} exceeds the Delta Catalog int64 range")]
    VersionOutOfWireRange {
        /// Internal unsigned version.
        actual: Version,
    },
    /// The stable catalog identity changed during the lifetime of an open table.
    #[error("Unity Catalog table identity changed from {expected} to {actual}")]
    TableIdentityChanged {
        /// Identity at open.
        expected: String,
        /// Identity returned later.
        actual: String,
    },
    /// The physical location changed during the lifetime of an open table.
    #[error("Unity Catalog table location changed from {expected} to {actual}")]
    TableLocationChanged {
        /// Location at open.
        expected: String,
        /// Location returned later.
        actual: String,
    },
    /// The catalog returned an invalid commit tail.
    #[error("Unity Catalog returned invalid managed commit state: {source}")]
    InvalidState {
        /// State validation failure.
        #[from]
        source: CatalogManagedStateError,
    },
    /// A write used a read-only governed open.
    #[error("a READ Unity Catalog open cannot commit")]
    ReadOnlyCommit,
    /// The core transaction did not provide the typed payload required by catalog coordination.
    #[error("catalog-managed commits require serialized bytes, typed actions, and prior metadata")]
    InvalidCommitPayload,
    /// The commit lacks a stable in-commit timestamp.
    #[error("catalog-managed commitInfo must contain inCommitTimestamp")]
    InCommitTimestampMissing,
    /// The commit transaction id does not identify the staged filename operation.
    #[error("catalog-managed commitInfo txnId {actual:?} does not match operation {expected}")]
    TransactionIdentityMismatch {
        /// Operation id assigned by delta-rs.
        expected: Uuid,
        /// Transaction id in CommitInfo.
        actual: Option<String>,
    },
    /// A commit carried more than one singleton metadata action.
    #[error("catalog-managed commit contains multiple {action} actions")]
    DuplicateMetadataAction {
        /// Duplicate action kind.
        action: &'static str,
    },
    /// Exact metadata removal cannot be computed without the read snapshot metadata.
    #[error("catalog-managed metadata change requires prior snapshot metadata")]
    PreviousMetadataMissing,
    /// The current Delta Catalog wire contract cannot represent clearing a table comment.
    #[error("Delta Catalog 1.0 cannot represent clearing an existing table comment")]
    CommentRemovalUnsupported,
    /// Unity Catalog's typed API does not support this system-controlled Delta domain.
    #[error("Delta Catalog 1.0 does not support system domain metadata {domain}")]
    DomainUnsupported {
        /// Unsupported Delta domain.
        domain: String,
    },
    /// Supported domain metadata did not match its declared JSON shape.
    #[error("Delta domain metadata {domain} is invalid: {source}")]
    InvalidDomainMetadata {
        /// Delta domain name.
        domain: String,
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// A staged commit filename was not valid UTF-8/path input.
    #[error("invalid staged commit path {path}: {source}")]
    InvalidStagedPath {
        /// Rejected object-store path.
        path: String,
        /// Object-store path parsing failure.
        #[source]
        source: PathError,
    },
    /// A caller supplied an independent storage identity alongside UC vending.
    #[error("caller-supplied storage identity option {option} is forbidden for uc:// access")]
    CallerStorageIdentity {
        /// Rejected storage option.
        option: String,
    },
    /// A required property asked the client to generate a value it was not given.
    #[error("Unity Catalog requires a value for property {key}, but neither side supplied one")]
    RequiredPropertyValueMissing {
        /// Property requiring a generated value.
        key: String,
    },
    /// A caller attempted to override a concrete catalog-required property.
    #[error("property {key} must equal catalog-required value {expected}, got {actual}")]
    RequiredPropertyConflict {
        /// Required property key.
        key: String,
        /// Catalog-required value.
        expected: String,
        /// Caller-supplied conflicting value.
        actual: String,
    },
    /// The finalized table did not preserve the staging allocation identity.
    #[error("finalized table identity {actual} does not match staging identity {expected}")]
    FinalizedIdentityMismatch {
        /// Staging UUID.
        expected: String,
        /// Final table UUID.
        actual: String,
    },
    /// The finalized table did not preserve the staging allocation location.
    #[error("finalized table location {actual} does not match staging location {expected}")]
    FinalizedLocationMismatch {
        /// Staging location.
        expected: String,
        /// Final table location.
        actual: String,
    },
}

/// Inputs to Unity Catalog managed-table creation.
#[derive(Debug, Clone)]
pub struct ManagedDeltaTableCreate {
    table: DeltaTableReference,
    schema: StructType,
    partition_columns: Vec<String>,
    properties: HashMap<String, String>,
    table_features: Vec<TableFeatures>,
    comment: Option<String>,
}

impl ManagedDeltaTableCreate {
    /// Construct a managed table request. Unity Catalog remains authoritative for name validity,
    /// allocation, required protocol, and required properties.
    pub fn new(
        table: DeltaTableReference,
        schema: StructType,
        partition_columns: Vec<String>,
        properties: HashMap<String, String>,
        comment: Option<String>,
    ) -> Self {
        Self {
            table,
            schema,
            partition_columns,
            properties,
            table_features: Vec::new(),
            comment,
        }
    }

    /// Add features the caller's table contract requires to the same
    /// version-zero protocol action as Unity Catalog's required features.
    pub fn with_table_features(
        mut self,
        features: impl IntoIterator<Item = TableFeatures>,
    ) -> Self {
        self.table_features = features.into_iter().collect();
        self
    }

    /// Three-part catalog reference being created.
    pub fn table(&self) -> &DeltaTableReference {
        &self.table
    }
}

#[derive(Debug, Clone)]
struct ResolvedTableState {
    table_uuid: String,
    location: String,
    etag: String,
    catalog_state: CatalogManagedState,
}

impl ResolvedTableState {
    fn try_from_load(load: &DeltaLoadTableResponse) -> Result<Self, CatalogManagedError> {
        if load.metadata.table_type != DeltaTableType::Managed {
            return Err(CatalogManagedError::TableType {
                actual: load.metadata.table_type,
            });
        }
        Uuid::parse_str(&load.metadata.table_uuid).map_err(|_| {
            CatalogManagedError::TableIdentityChanged {
                expected: String::from("a UUID"),
                actual: load.metadata.table_uuid.clone(),
            }
        })?;
        let latest_version = load
            .latest_table_version
            .ok_or(CatalogManagedError::LatestVersionMissing)?;
        let latest_version =
            Version::try_from(latest_version).map_err(|_| CatalogManagedError::InvalidVersion {
                field: "latest-table-version",
                actual: latest_version,
            })?;
        let mut commits = load.commits.clone();
        commits.reverse();
        let commits = commits
            .into_iter()
            .map(
                |commit| -> Result<CatalogManagedCommit, CatalogManagedError> {
                    let version = Version::try_from(commit.version).map_err(|_| {
                        CatalogManagedError::InvalidVersion {
                            field: "commit",
                            actual: commit.version,
                        }
                    })?;
                    CatalogManagedCommit::try_new(
                        version,
                        commit.file_name,
                        commit.timestamp,
                        commit.file_size,
                        commit.file_modification_timestamp,
                    )
                    .map_err(CatalogManagedError::from)
                },
            )
            .collect::<Result<Vec<_>, CatalogManagedError>>()?;
        Ok(Self {
            table_uuid: load.metadata.table_uuid.clone(),
            location: load.metadata.location.clone(),
            etag: load.metadata.etag.clone(),
            catalog_state: CatalogManagedState::try_new(latest_version, commits)?,
        })
    }

    fn validate_same_table(&self, next: &Self) -> Result<(), CatalogManagedError> {
        if self.table_uuid != next.table_uuid {
            return Err(CatalogManagedError::TableIdentityChanged {
                expected: self.table_uuid.clone(),
                actual: next.table_uuid.clone(),
            });
        }
        if self.location != next.location {
            return Err(CatalogManagedError::TableLocationChanged {
                expected: self.location.clone(),
                actual: next.location.clone(),
            });
        }
        Ok(())
    }
}

/// A Unity Catalog coordinated log store backed by delta-rs's native object-store implementation.
pub struct CatalogManagedLogStore {
    catalog: Arc<UnityCatalog>,
    table: DeltaTableReference,
    access: DeltaCredentialOperation,
    underlying: LogStoreRef,
    state: RwLock<ResolvedTableState>,
}

impl CatalogManagedLogStore {
    fn new(
        catalog: Arc<UnityCatalog>,
        table: DeltaTableReference,
        access: DeltaCredentialOperation,
        underlying: LogStoreRef,
        state: ResolvedTableState,
    ) -> Self {
        Self {
            catalog,
            table,
            access,
            underlying,
            state: RwLock::new(state),
        }
    }

    async fn refresh_catalog_state(&self) -> Result<ResolvedTableState, UnityCatalogError> {
        let load = self.catalog.load_delta_table(&self.table).await?;
        let next = ResolvedTableState::try_from_load(&load).map_err(delta_table_error)?;
        let current = self.state.read().await;
        current
            .validate_same_table(&next)
            .map_err(delta_table_error)?;
        drop(current);
        *self.state.write().await = next.clone();
        Ok(next)
    }

    async fn accept_load(
        &self,
        load: &DeltaLoadTableResponse,
    ) -> Result<ResolvedTableState, UnityCatalogError> {
        let next = ResolvedTableState::try_from_load(load).map_err(delta_table_error)?;
        let current = self.state.read().await;
        current
            .validate_same_table(&next)
            .map_err(delta_table_error)?;
        drop(current);
        *self.state.write().await = next.clone();
        Ok(next)
    }

    fn staged_path(file_name: &str) -> Result<Path, CatalogManagedError> {
        let raw = format!("{STAGED_COMMITS_DIRECTORY}/{file_name}");
        Path::parse(&raw)
            .map_err(|source| CatalogManagedError::InvalidStagedPath { path: raw, source })
    }

    async fn put_staged_commit(
        &self,
        version: Version,
        file_name: &str,
        bytes: &Bytes,
    ) -> Result<deltalake_core::logstore::object_store::ObjectMeta, TransactionError> {
        let path = Self::staged_path(file_name).map_err(transaction_error)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .underlying
            .object_store(None)
            .put_opts(&path, bytes.clone().into(), options)
            .await
        {
            Ok(_) => {}
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                let existing = self
                    .underlying
                    .object_store(None)
                    .get(&path)
                    .await?
                    .bytes()
                    .await?;
                if existing != *bytes {
                    return Err(TransactionError::VersionAlreadyExists(version));
                }
            }
            Err(source) => return Err(source.into()),
        }
        self.underlying
            .object_store(None)
            .head(&path)
            .await
            .map_err(TransactionError::from)
    }

    async fn reconcile_commit(
        &self,
        version: Version,
        file_name: &str,
        original: UnityCatalogError,
    ) -> Result<Option<ResolvedTableState>, TransactionError> {
        let wire_version = wire_version(version).map_err(transaction_error)?;
        match self.catalog.load_delta_table(&self.table).await {
            Ok(load) => {
                let state = self.accept_load(&load).await.map_err(transaction_error)?;
                let accepted = load
                    .commits
                    .iter()
                    .any(|commit| commit.version == wire_version && commit.file_name == file_name);
                if accepted {
                    return Ok(Some(state));
                }
                if state.catalog_state.latest_version() >= version {
                    return Err(TransactionError::VersionAlreadyExists(version));
                }
                Err(transaction_error(original))
            }
            Err(reconcile_error) => Err(TransactionError::LogStoreError {
                msg: CATALOG_COMMIT_FAILURE.to_owned(),
                source: Box::new(ReconciliationError {
                    original,
                    reconcile: reconcile_error,
                }),
            }),
        }
    }

    async fn publish_tail(
        &self,
        state: &ResolvedTableState,
    ) -> Result<ResolvedTableState, TransactionError> {
        let Some(last) = state.catalog_state.commits().last() else {
            return Ok(state.clone());
        };
        let store = self.underlying.object_store(None);
        for commit in state.catalog_state.commits() {
            let staged = Self::staged_path(commit.file_name()).map_err(transaction_error)?;
            let published = commit_uri_from_version(Some(commit.version()));
            match store.copy_if_not_exists(&staged, &published).await {
                Ok(()) => {}
                Err(ObjectStoreError::AlreadyExists { .. }) => {
                    let staged_bytes = store.get(&staged).await?.bytes().await?;
                    let published_bytes = store.get(&published).await?.bytes().await?;
                    if staged_bytes != published_bytes {
                        return Err(transaction_error(CatalogManagedError::InvalidCommitPayload));
                    }
                }
                Err(source) => return Err(source.into()),
            }
        }

        let request = DeltaUpdateTableRequest {
            requirements: vec![DeltaTableRequirement::AssertTableUuid {
                uuid: state.table_uuid.clone(),
            }],
            updates: vec![DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: wire_version(last.version())
                    .map_err(transaction_error)?,
            }],
        };
        let load = self
            .catalog
            .update_delta_table(&self.table, &request)
            .await
            .map_err(transaction_error)?;
        self.accept_load(&load).await.map_err(transaction_error)
    }

    async fn ratify(
        &self,
        version: Version,
        request: &DeltaUpdateTableRequest,
        file_name: &str,
    ) -> Result<ResolvedTableState, TransactionError> {
        match self.catalog.update_delta_table(&self.table, request).await {
            Ok(load) => self.accept_load(&load).await.map_err(transaction_error),
            Err(error) if error.delta_error_type() == Some("ResourceExhaustedException") => {
                let current = self
                    .refresh_catalog_state()
                    .await
                    .map_err(transaction_error)?;
                self.publish_tail(&current).await?;
                match self.catalog.update_delta_table(&self.table, request).await {
                    Ok(load) => self.accept_load(&load).await.map_err(transaction_error),
                    Err(retry_error) => self
                        .reconcile_commit(version, file_name, retry_error)
                        .await?
                        .ok_or_else(|| {
                            transaction_error(CatalogManagedError::InvalidCommitPayload)
                        }),
                }
            }
            Err(error) => self
                .reconcile_commit(version, file_name, error)
                .await?
                .ok_or_else(|| transaction_error(CatalogManagedError::InvalidCommitPayload)),
        }
    }
}

impl UnityCatalog {
    /// Create a UC-managed Delta table through the staging protocol and return it opened through
    /// the catalog-coordinated log store. Version zero is written by delta-rs's existing
    /// [`CreateBuilder`]; this method supplies the protocol and properties mandated by UC and does
    /// not implement Delta creation independently.
    pub async fn create_managed_delta_table(
        self: &Arc<Self>,
        request: ManagedDeltaTableCreate,
        storage_config: &StorageConfig,
    ) -> DeltaResult<DeltaTable> {
        self.delta_catalog_config(request.table.catalog())
            .await?
            .require(&[
                endpoint::CREATE_STAGING_TABLE,
                endpoint::CREATE_TABLE,
                endpoint::STAGING_CREDENTIALS,
                endpoint::LOAD_TABLE,
                endpoint::TABLE_CREDENTIALS,
                endpoint::UPDATE_TABLE,
            ])?;

        let staging = self.create_delta_staging_table(&request.table).await?;
        if staging.table_type != DeltaTableType::Managed {
            return Err(managed_delta_error(CatalogManagedError::TableType {
                actual: staging.table_type,
            }));
        }
        Uuid::parse_str(&staging.table_id).map_err(|_| {
            managed_delta_error(CatalogManagedError::FinalizedIdentityMismatch {
                expected: String::from("a UUID"),
                actual: staging.table_id.clone(),
            })
        })?;

        let properties =
            merge_required_properties(&request.properties, &staging.required_properties)
                .map_err(managed_delta_error)?;
        let protocol = protocol_with_table_features(
            protocol_from_wire(&staging.required_protocol)?,
            &request.table_features,
        );
        PROTOCOL.can_write_to_protocol(&protocol)?;

        let initial_credentials = DeltaCredentialsResponse {
            storage_credentials: staging.storage_credentials.clone(),
        };
        let vended = match initial_credentials
            .storage_options_for(&staging.location, DeltaCredentialOperation::ReadWrite)
        {
            Ok(vended) => vended,
            Err(_) => self
                .delta_staging_credentials(&staging.table_id)
                .await?
                .storage_options_for(&staging.location, DeltaCredentialOperation::ReadWrite)?,
        };
        let underlying = native_log_store(&staging.location, storage_config, vended.options())?;

        let timestamp = chrono::Utc::now().timestamp_millis();
        let operation_id = Uuid::new_v4();
        let commit_info = CommitInfo {
            timestamp: Some(timestamp),
            in_commit_timestamp: Some(timestamp),
            txn_id: Some(operation_id.to_string()),
            operation: Some(String::from("CREATE TABLE")),
            engine_info: Some(format!("delta-rs:{}", crate_version())),
            ..CommitInfo::default()
        };
        let mut create = CreateBuilder::new()
            .with_table_name(request.table.table().to_owned())
            .with_log_store(underlying.clone())
            .with_columns(request.schema.fields().cloned())
            .with_partition_columns(request.partition_columns.clone())
            .with_configuration(
                properties
                    .iter()
                    .map(|(key, value)| (key.clone(), Some(value.clone()))),
            )
            .with_raise_if_key_not_exists(false)
            .with_actions([Action::Protocol(protocol), Action::CommitInfo(commit_info)]);
        if let Some(comment) = &request.comment {
            create = create.with_comment(comment.clone());
        }
        let created = create.commit_new_table_without_load().await?;
        let create_request = DeltaCreateTableRequest {
            name: request.table.table().to_owned(),
            location: staging.location.clone(),
            table_type: DeltaTableType::Managed,
            comment: request.comment,
            columns: created.metadata().parse_schema()?.into(),
            partition_columns: created.metadata().partition_columns().to_vec(),
            protocol: protocol_to_wire(created.protocol()),
            properties: created.metadata().configuration().clone(),
            last_commit_timestamp_ms: timestamp,
        };
        let finalized = match self
            .create_delta_table(&request.table, &create_request)
            .await
        {
            Ok(load) => load,
            Err(original) => match self.load_delta_table(&request.table).await {
                Ok(load) => load,
                Err(reconcile) => {
                    return Err(DeltaTableError::GenericError {
                        source: Box::new(ReconciliationError {
                            original,
                            reconcile,
                        }),
                    });
                }
            },
        };
        validate_finalized_table(&staging.table_id, &staging.location, &finalized)
            .map_err(managed_delta_error)?;
        let state = ResolvedTableState::try_from_load(&finalized).map_err(managed_delta_error)?;
        let log_store: LogStoreRef = Arc::new(CatalogManagedLogStore::new(
            self.clone(),
            request.table,
            DeltaCredentialOperation::ReadWrite,
            underlying,
            state,
        ));
        let mut table = DeltaTable::new(log_store, Default::default());
        table.load().await?;
        Ok(table)
    }
}

#[async_trait::async_trait]
impl LogStore for CatalogManagedLogStore {
    fn name(&self) -> String {
        CATALOG_MANAGED_STORE_NAME.to_owned()
    }

    fn is_catalog_managed(&self) -> bool {
        true
    }

    fn commit_payload_mode(&self) -> CommitPayloadMode {
        CommitPayloadMode::LogBytesWithActions
    }

    async fn refresh(&self) -> DeltaResult<()> {
        self.underlying.refresh().await?;
        self.refresh_catalog_state().await?;
        Ok(())
    }

    async fn catalog_managed_state(&self) -> DeltaResult<Option<CatalogManagedState>> {
        let state = self.refresh_catalog_state().await?;
        Ok(Some(state.catalog_state))
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        let state = self.state.read().await;
        let commit = state
            .catalog_state
            .commits()
            .iter()
            .find(|commit| commit.version() == version)
            .cloned();
        drop(state);
        if let Some(commit) = commit {
            let path = Self::staged_path(commit.file_name()).map_err(delta_table_error)?;
            return self
                .underlying
                .object_store(None)
                .get(&path)
                .await
                .map_err(DeltaTableError::from)?
                .bytes()
                .await
                .map(Some)
                .map_err(DeltaTableError::from);
        }
        self.underlying.read_commit_entry(version).await
    }

    async fn write_commit_entry(
        &self,
        version: Version,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        if self.access != DeltaCredentialOperation::ReadWrite {
            return Err(transaction_error(CatalogManagedError::ReadOnlyCommit));
        }
        let CommitOrBytes::LogBytesWithActions {
            bytes,
            actions,
            previous_metadata,
        } = commit_or_bytes
        else {
            return Err(transaction_error(CatalogManagedError::InvalidCommitPayload));
        };

        let current = self
            .refresh_catalog_state()
            .await
            .map_err(transaction_error)?;
        if version != current.catalog_state.latest_version() + 1 {
            return Err(TransactionError::VersionAlreadyExists(version));
        }
        let file_name = format!("{version:020}.{operation_id}.json");
        let object_meta = self.put_staged_commit(version, &file_name, &bytes).await?;
        let request = update_request(
            version,
            file_name.clone(),
            &object_meta,
            &actions,
            previous_metadata.as_ref(),
            &current,
            operation_id,
        )?;
        let accepted = self.ratify(version, &request, &file_name).await?;

        if let Err(source) = self.publish_tail(&accepted).await {
            error!(
                table = %self.table.table(),
                version,
                error = %source,
                "catalog ratified the commit but immediate publication did not complete"
            );
        }
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        _version: Version,
        _commit_or_bytes: CommitOrBytes,
        _operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        // A failed client response can race with catalog ratification. Deleting the staged file
        // here could therefore destroy an authoritative commit. Publication/backfill owns cleanup.
        Ok(())
    }

    async fn get_latest_version(&self, _start_version: Version) -> DeltaResult<Version> {
        Ok(self
            .refresh_catalog_state()
            .await?
            .catalog_state
            .latest_version())
    }

    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.underlying.object_store(operation_id)
    }

    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.underlying.root_object_store(operation_id)
    }

    fn transaction_url(&self, operation_id: Option<Uuid>) -> DeltaResult<Url> {
        self.underlying.transaction_url(operation_id)
    }

    fn config(&self) -> &LogStoreConfig {
        self.underlying.config()
    }
}

#[derive(thiserror::Error, Debug)]
#[error(
    "catalog commit outcome reconciliation failed after {original}; reload failed with {reconcile}"
)]
struct ReconciliationError {
    original: UnityCatalogError,
    reconcile: UnityCatalogError,
}

fn update_request(
    version: Version,
    file_name: String,
    object_meta: &deltalake_core::logstore::object_store::ObjectMeta,
    actions: &[Action],
    previous_metadata: Option<&Metadata>,
    state: &ResolvedTableState,
    operation_id: Uuid,
) -> Result<DeltaUpdateTableRequest, TransactionError> {
    let commit_info = actions
        .iter()
        .find_map(|action| match action {
            Action::CommitInfo(info) => Some(info),
            _ => None,
        })
        .ok_or_else(|| transaction_error(CatalogManagedError::InCommitTimestampMissing))?;
    validate_commit_info(commit_info, operation_id)?;

    let timestamp = commit_info
        .in_commit_timestamp
        .ok_or_else(|| transaction_error(CatalogManagedError::InCommitTimestampMissing))?;
    let commit = DeltaCommit {
        version: wire_version(version).map_err(transaction_error)?,
        timestamp,
        file_name,
        file_size: object_meta.size,
        file_modification_timestamp: object_meta.last_modified.timestamp_millis(),
    };

    let mut updates = metadata_updates(actions, previous_metadata)?;
    let metadata_changed = !updates.is_empty();
    updates.push(DeltaTableUpdate::AddCommit { commit });
    let mut requirements = vec![DeltaTableRequirement::AssertTableUuid {
        uuid: state.table_uuid.clone(),
    }];
    if metadata_changed {
        requirements.push(DeltaTableRequirement::AssertEtag {
            etag: state.etag.clone(),
        });
    }
    Ok(DeltaUpdateTableRequest {
        requirements,
        updates,
    })
}

fn validate_commit_info(
    commit_info: &CommitInfo,
    operation_id: Uuid,
) -> Result<(), TransactionError> {
    let actual = commit_info
        .txn_id
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok());
    if actual != Some(operation_id) {
        return Err(transaction_error(
            CatalogManagedError::TransactionIdentityMismatch {
                expected: operation_id,
                actual: commit_info.txn_id.clone(),
            },
        ));
    }
    Ok(())
}

fn metadata_updates(
    actions: &[Action],
    previous_metadata: Option<&Metadata>,
) -> Result<Vec<DeltaTableUpdate>, TransactionError> {
    let protocols: Vec<&Protocol> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Protocol(protocol) => Some(protocol),
            _ => None,
        })
        .collect();
    if protocols.len() > 1 {
        return Err(transaction_error(
            CatalogManagedError::DuplicateMetadataAction { action: "protocol" },
        ));
    }
    let metadata_actions: Vec<&Metadata> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Metadata(metadata) => Some(metadata),
            _ => None,
        })
        .collect();
    if metadata_actions.len() > 1 {
        return Err(transaction_error(
            CatalogManagedError::DuplicateMetadataAction { action: "metadata" },
        ));
    }

    let mut updates = Vec::new();
    if let Some(protocol) = protocols.first() {
        updates.push(DeltaTableUpdate::SetProtocol {
            protocol: protocol_to_wire(protocol),
        });
    }
    if let Some(metadata) = metadata_actions.first() {
        append_metadata_updates(&mut updates, metadata, previous_metadata)?;
    }
    append_domain_updates(&mut updates, actions)?;
    Ok(updates)
}

fn append_metadata_updates(
    updates: &mut Vec<DeltaTableUpdate>,
    metadata: &Metadata,
    previous_metadata: Option<&Metadata>,
) -> Result<(), TransactionError> {
    let previous = previous_metadata
        .ok_or_else(|| transaction_error(CatalogManagedError::PreviousMetadataMissing))?;
    let schema = metadata.parse_schema().map_err(transaction_error)?;
    updates.push(DeltaTableUpdate::SetColumns {
        columns: schema.into(),
    });
    updates.push(DeltaTableUpdate::SetPartitionColumns {
        partition_columns: metadata.partition_columns().to_vec(),
    });

    match (previous.description(), metadata.description()) {
        (Some(_), None) => {
            return Err(transaction_error(
                CatalogManagedError::CommentRemovalUnsupported,
            ));
        }
        (before, Some(after)) if before != Some(after) => {
            updates.push(DeltaTableUpdate::SetTableComment {
                comment: after.to_owned(),
            });
        }
        _ => {}
    }

    let set_properties: HashMap<String, String> = metadata
        .configuration()
        .iter()
        .filter(|(key, value)| previous.configuration().get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if !set_properties.is_empty() {
        updates.push(DeltaTableUpdate::SetProperties {
            updates: set_properties,
        });
    }
    let removals: Vec<String> = previous
        .configuration()
        .keys()
        .filter(|key| !metadata.configuration().contains_key(*key))
        .cloned()
        .collect();
    if !removals.is_empty() {
        updates.push(DeltaTableUpdate::RemoveProperties { removals });
    }
    Ok(())
}

fn append_domain_updates(
    updates: &mut Vec<DeltaTableUpdate>,
    actions: &[Action],
) -> Result<(), TransactionError> {
    let mut set = DeltaDomainMetadataUpdates::default();
    let mut removals = Vec::new();
    for domain in actions.iter().filter_map(|action| match action {
        Action::DomainMetadata(domain) => Some(domain),
        _ => None,
    }) {
        if domain.removed {
            if is_catalog_managed_domain(&domain.domain) {
                removals.push(domain.domain.clone());
            } else if domain.domain.starts_with(SYSTEM_DOMAIN_PREFIX) {
                return Err(transaction_error(CatalogManagedError::DomainUnsupported {
                    domain: domain.domain.clone(),
                }));
            }
            continue;
        }
        match domain.domain.as_str() {
            CLUSTERING_DOMAIN => {
                set.clustering = Some(parse_domain::<DeltaClusteringDomainMetadata>(domain)?);
            }
            ROW_TRACKING_DOMAIN => {
                set.row_tracking = Some(parse_domain::<DeltaRowTrackingDomainMetadata>(domain)?);
            }
            unsupported if unsupported.starts_with(SYSTEM_DOMAIN_PREFIX) => {
                return Err(transaction_error(CatalogManagedError::DomainUnsupported {
                    domain: domain.domain.clone(),
                }));
            }
            // User-controlled domains live in the ratified Delta action and
            // are checkpointed by Delta. Unity Catalog's update wire has
            // typed fields only for the system domains it itself maintains;
            // duplicating an application domain into catalog properties is
            // neither required by Delta nor representable by that contract.
            _ => {}
        }
    }
    if set != DeltaDomainMetadataUpdates::default() {
        updates.push(DeltaTableUpdate::SetDomainMetadata { updates: set });
    }
    if !removals.is_empty() {
        updates.push(DeltaTableUpdate::RemoveDomainMetadata { domains: removals });
    }
    Ok(())
}

fn is_catalog_managed_domain(domain: &str) -> bool {
    matches!(domain, CLUSTERING_DOMAIN | ROW_TRACKING_DOMAIN)
}

fn parse_domain<T: serde::de::DeserializeOwned>(
    domain: &DomainMetadata,
) -> Result<T, TransactionError> {
    serde_json::from_str(&domain.configuration).map_err(|source| {
        transaction_error(CatalogManagedError::InvalidDomainMetadata {
            domain: domain.domain.clone(),
            source,
        })
    })
}

fn protocol_to_wire(protocol: &Protocol) -> DeltaProtocol {
    DeltaProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: protocol.reader_features().map(|features| {
            features
                .iter()
                .map(|feature| feature.as_ref().to_owned())
                .collect()
        }),
        writer_features: protocol.writer_features().map(|features| {
            features
                .iter()
                .map(|feature| feature.as_ref().to_owned())
                .collect()
        }),
    }
}

fn wire_version(version: Version) -> Result<i64, CatalogManagedError> {
    i64::try_from(version)
        .map_err(|_| CatalogManagedError::VersionOutOfWireRange { actual: version })
}

fn transaction_error(source: impl std::error::Error + Send + Sync + 'static) -> TransactionError {
    TransactionError::LogStoreError {
        msg: CATALOG_COMMIT_FAILURE.to_owned(),
        source: Box::new(source),
    }
}

fn delta_table_error(source: impl std::error::Error + Send + Sync + 'static) -> UnityCatalogError {
    UnityCatalogError::Generic {
        source: Box::new(source),
    }
}

fn managed_delta_error(source: impl std::error::Error + Send + Sync + 'static) -> DeltaTableError {
    DeltaTableError::GenericError {
        source: Box::new(source),
    }
}

fn merge_required_properties(
    supplied: &HashMap<String, String>,
    required: &HashMap<String, Option<String>>,
) -> Result<HashMap<String, String>, CatalogManagedError> {
    let mut merged = supplied.clone();
    for (key, required_value) in required {
        match (merged.get(key), required_value) {
            (Some(actual), Some(expected)) if actual != expected => {
                return Err(CatalogManagedError::RequiredPropertyConflict {
                    key: key.clone(),
                    expected: expected.clone(),
                    actual: actual.clone(),
                });
            }
            (_, Some(expected)) => {
                merged.insert(key.clone(), expected.clone());
            }
            (Some(_), None) => {}
            (None, None) => {
                return Err(CatalogManagedError::RequiredPropertyValueMissing { key: key.clone() });
            }
        }
    }
    Ok(merged)
}

fn protocol_from_wire(protocol: &DeltaProtocol) -> DeltaResult<Protocol> {
    serde_json::from_value(serde_json::json!({
        "minReaderVersion": protocol.min_reader_version,
        "minWriterVersion": protocol.min_writer_version,
        "readerFeatures": protocol.reader_features,
        "writerFeatures": protocol.writer_features,
    }))
    .map_err(DeltaTableError::from)
}

fn validate_finalized_table(
    table_id: &str,
    location: &str,
    load: &DeltaLoadTableResponse,
) -> Result<(), CatalogManagedError> {
    if load.metadata.table_uuid != table_id {
        return Err(CatalogManagedError::FinalizedIdentityMismatch {
            expected: table_id.to_owned(),
            actual: load.metadata.table_uuid.clone(),
        });
    }
    if load.metadata.location.trim_end_matches('/') != location.trim_end_matches('/') {
        return Err(CatalogManagedError::FinalizedLocationMismatch {
            expected: location.to_owned(),
            actual: load.metadata.location.clone(),
        });
    }
    Ok(())
}

fn requested_access(
    options: &StorageConfig,
) -> Result<DeltaCredentialOperation, UnityCatalogError> {
    options
        .raw
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(UNITY_CATALOG_ACCESS_KEY))
        .map(|(_, value)| DeltaCredentialOperation::from_str(value))
        .unwrap_or_else(|| {
            Err(UnityCatalogError::InvalidDeltaAccessIntent {
                actual: "missing".to_owned(),
            })
        })
}

fn transport_storage_options(
    options: &StorageConfig,
) -> Result<HashMap<String, String>, UnityCatalogError> {
    let mut transport = HashMap::new();
    for (key, value) in &options.raw {
        if key.eq_ignore_ascii_case(UNITY_CATALOG_ACCESS_KEY)
            || UnityCatalogConfigKey::from_str(key).is_ok()
        {
            continue;
        }
        if CALLER_STORAGE_IDENTITY_OPTIONS
            .iter()
            .any(|forbidden| key.eq_ignore_ascii_case(forbidden))
        {
            return Err(delta_table_error(
                CatalogManagedError::CallerStorageIdentity {
                    option: key.clone(),
                },
            ));
        }
        transport.insert(key.clone(), value.clone());
    }
    Ok(transport)
}

fn native_log_store(
    location: &str,
    storage_config: &StorageConfig,
    vended_options: &HashMap<String, String>,
) -> DeltaResult<LogStoreRef> {
    let mut storage_options = transport_storage_options(storage_config)?;
    storage_options.extend(vended_options.clone());

    let location = ensure_table_uri(location)?;
    let mut native_builder = DeltaTableBuilder::from_url(location)?;
    if let Some(runtime) = &storage_config.runtime {
        native_builder = native_builder.with_io_runtime(runtime.clone());
    }
    native_builder = native_builder.with_storage_options(storage_options);
    native_builder.build_storage()
}

pub(crate) async fn build_catalog_managed_log_store(
    table_uri: &str,
    options: &StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let table = DeltaTableReference::try_from_uri(table_uri)?;
    let access = requested_access(options)?;
    let mut builder = UnityCatalogBuilder::from_env();
    builder = builder
        .try_with_options(
            options
                .raw
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .map_err(UnityCatalogError::from)?;
    let catalog = Arc::new(builder.build().map_err(UnityCatalogError::from)?);
    let required_endpoints: &[&str] = match access {
        DeltaCredentialOperation::Read => &[endpoint::LOAD_TABLE, endpoint::TABLE_CREDENTIALS],
        DeltaCredentialOperation::ReadWrite => &[
            endpoint::LOAD_TABLE,
            endpoint::TABLE_CREDENTIALS,
            endpoint::UPDATE_TABLE,
        ],
    };
    catalog
        .delta_catalog_config(table.catalog())
        .await?
        .require(required_endpoints)?;
    let load = catalog.load_delta_table(&table).await?;
    let state = ResolvedTableState::try_from_load(&load).map_err(delta_table_error)?;
    let credentials = catalog.delta_table_credentials(&table, access).await?;
    let vended = credentials.storage_options_for(&state.location, access)?;
    let underlying = native_log_store(&state.location, options, vended.options())?;
    Ok(Arc::new(CatalogManagedLogStore::new(
        catalog, table, access, underlying, state,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::kernel::{DataType, Protocol, StructField, new_metadata};

    #[cfg(feature = "aws")]
    #[test]
    fn unity_registration_installs_native_s3_handlers_before_factory_use() {
        crate::register_handlers(None);
        let s3 = Url::parse("s3://").expect("the native S3 scheme is valid");

        assert!(
            deltalake_core::logstore::object_store_factories().contains_key(&s3),
            "the native S3 object-store factory must precede the UC factory callback"
        );
        assert!(
            deltalake_core::logstore::logstore_factories().contains_key(&s3),
            "the native S3 log-store factory must precede the UC factory callback"
        );
    }

    #[test]
    fn protocol_conversion_preserves_versions_and_features() {
        let protocol: Protocol = serde_json::from_value(serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged", "v2Checkpoint"],
            "writerFeatures": ["catalogManaged", "inCommitTimestamp", "v2Checkpoint"]
        }))
        .expect("valid protocol");
        let wire = protocol_to_wire(&protocol);
        assert_eq!(wire.min_reader_version, 3);
        assert_eq!(wire.min_writer_version, 7);
        assert_eq!(
            wire.reader_features,
            Some(vec!["catalogManaged".to_owned(), "v2Checkpoint".to_owned()])
        );
    }

    #[test]
    fn catalog_state_rejects_a_non_contiguous_tail() {
        let load = DeltaLoadTableResponse {
            metadata: crate::delta::DeltaTableMetadata {
                etag: "etag".to_owned(),
                table_type: DeltaTableType::Managed,
                table_uuid: Uuid::new_v4().to_string(),
                location: "s3://bucket/table".to_owned(),
                created_time: 0,
                updated_time: 0,
                columns: serde_json::json!({"type": "struct", "fields": []}),
                partition_columns: Vec::new(),
                properties: HashMap::new(),
                last_commit_version: Some(0),
                last_commit_timestamp_ms: Some(0),
            },
            commits: vec![
                DeltaCommit {
                    version: 3,
                    timestamp: 3,
                    file_name: format!("{:020}.{}.json", 3, Uuid::new_v4()),
                    file_size: 1,
                    file_modification_timestamp: 3,
                },
                DeltaCommit {
                    version: 1,
                    timestamp: 1,
                    file_name: format!("{:020}.{}.json", 1, Uuid::new_v4()),
                    file_size: 1,
                    file_modification_timestamp: 1,
                },
            ],
            latest_table_version: Some(3),
        };
        assert!(ResolvedTableState::try_from_load(&load).is_err());
    }

    #[test]
    fn metadata_updates_are_derived_from_delta_metadata_not_catalog_properties() {
        let schema = StructType::try_new([StructField::new("id", DataType::LONG, false)])
            .expect("valid test schema");
        let previous = new_metadata(
            &schema,
            Vec::<String>::new(),
            [("removed", "old"), ("changed", "old")],
        )
        .expect("valid prior metadata");
        let current = new_metadata(
            &schema,
            Vec::<String>::new(),
            [("changed", "new"), ("added", "new")],
        )
        .expect("valid current metadata");
        let updates = metadata_updates(&[Action::Metadata(current)], Some(&previous))
            .expect("metadata diff is representable");

        assert!(updates.iter().any(|update| matches!(
            update,
            DeltaTableUpdate::SetProperties { updates }
                if updates == &HashMap::from([
                    ("changed".to_owned(), "new".to_owned()),
                    ("added".to_owned(), "new".to_owned()),
                ])
        )));
        assert!(updates.iter().any(|update| matches!(
            update,
            DeltaTableUpdate::RemoveProperties { removals }
                if removals == &vec!["removed".to_owned()]
        )));
    }

    #[test]
    fn catalog_required_properties_cannot_be_weakened_or_invented() {
        let required = HashMap::from([
            ("fixed".to_owned(), Some("required".to_owned())),
            ("generated".to_owned(), None),
        ]);
        assert!(merge_required_properties(&HashMap::new(), &required).is_err());
        assert!(
            merge_required_properties(
                &HashMap::from([
                    ("fixed".to_owned(), "other".to_owned()),
                    ("generated".to_owned(), "value".to_owned()),
                ]),
                &required,
            )
            .is_err()
        );
        let merged = merge_required_properties(
            &HashMap::from([("generated".to_owned(), "value".to_owned())]),
            &required,
        )
        .expect("the catalog fixes one value and the caller supplies the generated one");
        assert_eq!(merged["fixed"], "required");
        assert_eq!(merged["generated"], "value");
    }
}
