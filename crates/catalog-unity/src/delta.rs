//! Unity Catalog's Delta Catalog API wire contract.
//!
//! This module owns only catalog protocol negotiation, table metadata, coordinated-commit
//! requests, and conversion of catalog-vended temporary credentials into delta-rs storage
//! options. Delta log parsing, conflict detection, object-store I/O, and query execution remain
//! in their existing delta-rs domains.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use chrono::Utc;
use deltalake_core::kernel::{
    DataType, MetadataValue, StructField, StructType as KernelStructType,
};
use reqwest::Url;
use reqwest::header::AUTHORIZATION;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};

use crate::{UnityCatalog, UnityCatalogError};

/// Storage option that tells the Unity Catalog factory which permission to request.
pub const UNITY_CATALOG_ACCESS_KEY: &str = "unity_catalog_access";

const DELTA_PROTOCOL_VERSION: &str = "1.0";
const DELTA_API_PATH: &str = "/api/2.1/unity-catalog/delta/v1/";
const ERROR_TYPE_UNAVAILABLE: &str = "UnparseableErrorResponse";
const ERROR_MESSAGE_UNAVAILABLE: &str = "The response body did not match DeltaErrorResponse.";

/// Endpoint signatures advertised by protocol negotiation.
pub mod endpoint {
    /// Load an existing table.
    pub const LOAD_TABLE: &str = "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    /// Update table metadata or ratify a managed commit.
    pub const UPDATE_TABLE: &str = "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";
    /// Vend credentials for an existing table.
    pub const TABLE_CREDENTIALS: &str =
        "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials";
    /// Allocate a managed staging table.
    pub const CREATE_STAGING_TABLE: &str =
        "POST /v1/catalogs/{catalog}/schemas/{schema}/staging-tables";
    /// Finalize a table after its initial Delta commit is written.
    pub const CREATE_TABLE: &str = "POST /v1/catalogs/{catalog}/schemas/{schema}/tables";
    /// Re-vend credentials for a staging table.
    pub const STAGING_CREDENTIALS: &str = "GET /v1/staging-tables/{table_id}/credentials";
}

/// Explicit storage permission requested from Unity Catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeltaCredentialOperation {
    /// Read table metadata and data.
    Read,
    /// Read and write table metadata and data.
    ReadWrite,
}

impl DeltaCredentialOperation {
    /// Unity Catalog wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::ReadWrite => "READ_WRITE",
        }
    }
}

impl fmt::Display for DeltaCredentialOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DeltaCredentialOperation {
    type Err = UnityCatalogError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "READ" => Ok(Self::Read),
            "READ_WRITE" => Ok(Self::ReadWrite),
            actual => Err(UnityCatalogError::InvalidDeltaAccessIntent {
                actual: actual.to_owned(),
            }),
        }
    }
}

/// A three-part table reference accepted by the `uc://` delta-rs factory.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeltaTableReference {
    catalog: String,
    schema: String,
    table: String,
}

impl DeltaTableReference {
    /// Parse `uc://catalog.schema.table` without applying catalog naming policy locally.
    pub fn try_from_uri(table_uri: &str) -> Result<Self, UnityCatalogError> {
        let parsed =
            Url::parse(table_uri).map_err(|_| UnityCatalogError::InvalidDeltaTableReference {
                table_uri: table_uri.to_owned(),
            })?;
        if parsed.scheme() != "uc"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(UnityCatalogError::InvalidDeltaTableReference {
                table_uri: table_uri.to_owned(),
            });
        }
        let qualified_name =
            parsed
                .host_str()
                .ok_or_else(|| UnityCatalogError::InvalidDeltaTableReference {
                    table_uri: table_uri.to_owned(),
                })?;
        let parts: Vec<&str> = qualified_name.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(UnityCatalogError::InvalidDeltaTableReference {
                table_uri: table_uri.to_owned(),
            });
        }
        Ok(Self {
            catalog: parts[0].to_owned(),
            schema: parts[1].to_owned(),
            table: parts[2].to_owned(),
        })
    }

    /// Construct a reference already obtained from a typed caller boundary.
    pub fn new(catalog: String, schema: String, table: String) -> Result<Self, UnityCatalogError> {
        if catalog.is_empty() || schema.is_empty() || table.is_empty() {
            return Err(UnityCatalogError::InvalidDeltaTableReference {
                table_uri: format!("uc://{catalog}.{schema}.{table}"),
            });
        }
        Ok(Self {
            catalog,
            schema,
            table,
        })
    }

    /// Catalog name.
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Table name.
    pub fn table(&self) -> &str {
        &self.table
    }
}

/// Delta Catalog protocol negotiation response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaCatalogConfig {
    /// Supported endpoint signatures.
    pub endpoints: Vec<String>,
    /// Selected protocol version.
    pub protocol_version: String,
}

impl DeltaCatalogConfig {
    /// Fail closed unless the server selected the protocol implemented here and advertises every
    /// endpoint needed by the requested operation.
    pub fn require(&self, endpoints: &[&'static str]) -> Result<(), UnityCatalogError> {
        if self.protocol_version != DELTA_PROTOCOL_VERSION {
            return Err(UnityCatalogError::UnsupportedDeltaProtocol {
                expected: DELTA_PROTOCOL_VERSION,
                actual: self.protocol_version.clone(),
            });
        }
        for endpoint in endpoints {
            if !self.endpoints.iter().any(|candidate| candidate == endpoint) {
                return Err(UnityCatalogError::MissingDeltaEndpoint { endpoint });
            }
        }
        Ok(())
    }
}

/// Managed or external table classification assigned by Unity Catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeltaTableType {
    /// Catalog-allocated and catalog-coordinated storage.
    Managed,
    /// Caller-owned storage.
    External,
}

/// Delta protocol representation used by the catalog wire contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaProtocol {
    /// Minimum reader protocol version.
    pub min_reader_version: i32,
    /// Minimum writer protocol version.
    pub min_writer_version: i32,
    /// Reader table features.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<Vec<String>>,
    /// Writer table features.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<Vec<String>>,
}

/// One catalog-ratified CCv2 commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaCommit {
    /// Ratified Delta version.
    pub version: i64,
    /// In-commit timestamp in epoch milliseconds.
    pub timestamp: i64,
    /// Filename under `_delta_log/_staged_commits`.
    pub file_name: String,
    /// Serialized size in bytes.
    pub file_size: u64,
    /// Object modification time in epoch milliseconds.
    pub file_modification_timestamp: i64,
}

/// Catalog metadata returned atomically with the unbackfilled commit tail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaTableMetadata {
    /// Optimistic metadata concurrency token.
    pub etag: String,
    /// Catalog table classification.
    pub table_type: DeltaTableType,
    /// Stable table identity.
    pub table_uuid: String,
    /// Physical Delta table location.
    pub location: String,
    /// Creation time in epoch milliseconds.
    pub created_time: i64,
    /// Last catalog metadata update time in epoch milliseconds.
    pub updated_time: i64,
    /// Delta schema JSON object.
    pub columns: serde_json::Value,
    /// Logical partition columns.
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Delta properties plus server-derived read-only properties.
    pub properties: HashMap<String, String>,
    /// Last metadata-changing commit version.
    #[serde(default)]
    pub last_commit_version: Option<i64>,
    /// Last metadata-changing commit timestamp.
    #[serde(default)]
    pub last_commit_timestamp_ms: Option<i64>,
}

/// Atomic table load response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaLoadTableResponse {
    /// Complete catalog metadata.
    pub metadata: DeltaTableMetadata,
    /// Complete descending unbackfilled commit tail.
    #[serde(default)]
    pub commits: Vec<DeltaCommit>,
    /// Latest ratified version, including data-only commits.
    #[serde(default)]
    pub latest_table_version: Option<i64>,
}

/// One temporary storage credential and its exact scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaStorageCredential {
    /// Storage prefix this credential may access.
    pub prefix: String,
    /// Permission granted by the credential.
    pub operation: DeltaCredentialOperation,
    /// Provider-specific secret fields.
    pub config: HashMap<String, String>,
    /// Credential expiration in epoch milliseconds.
    pub expiration_time_ms: i64,
}

/// Credential vending response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaCredentialsResponse {
    /// Candidate credentials; the client selects the longest matching prefix.
    pub storage_credentials: Vec<DeltaStorageCredential>,
}

/// Validated delta-rs storage options derived from one temporary S3 credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VendedStorageOptions {
    options: HashMap<String, String>,
    prefix: String,
    expiration_time_ms: i64,
}

impl VendedStorageOptions {
    /// Provider options consumed by delta-rs's existing object-store factory.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Exact storage prefix authorized by Unity Catalog.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Credential expiration in epoch milliseconds.
    pub fn expiration_time_ms(&self) -> i64 {
        self.expiration_time_ms
    }
}

impl DeltaCredentialsResponse {
    /// Select the longest matching, unexpired credential with exactly the requested permission.
    /// Only the S3 temporary-session shape needed by the Ceph/AWS delta-rs backend is accepted.
    pub fn storage_options_for(
        &self,
        location: &str,
        operation: DeltaCredentialOperation,
    ) -> Result<VendedStorageOptions, UnityCatalogError> {
        let location_prefix = format!("{}/", location.trim_end_matches('/'));
        let now = Utc::now().timestamp_millis();
        let credential = self
            .storage_credentials
            .iter()
            .filter(|credential| credential.operation == operation)
            .filter(|credential| credential.expiration_time_ms > now)
            .filter(|credential| {
                let candidate = format!("{}/", credential.prefix.trim_end_matches('/'));
                location_prefix.starts_with(&candidate)
            })
            .max_by_key(|credential| credential.prefix.len())
            .ok_or(UnityCatalogError::InvalidDeltaCredential {
                reason: "no_matching_unexpired_credential",
            })?;

        let access_key = required_secret(&credential.config, "s3.access-key-id")?;
        let secret_key = required_secret(&credential.config, "s3.secret-access-key")?;
        let session_token = required_secret(&credential.config, "s3.session-token")?;
        let options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".to_owned(), access_key.to_owned()),
            ("AWS_SECRET_ACCESS_KEY".to_owned(), secret_key.to_owned()),
            ("AWS_SESSION_TOKEN".to_owned(), session_token.to_owned()),
        ]);
        Ok(VendedStorageOptions {
            options,
            prefix: credential.prefix.clone(),
            expiration_time_ms: credential.expiration_time_ms,
        })
    }
}

fn required_secret<'a>(
    config: &'a HashMap<String, String>,
    key: &'static str,
) -> Result<&'a str, UnityCatalogError> {
    config
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or(UnityCatalogError::InvalidDeltaCredential { reason: key })
}

/// Allocate a managed table's identity and storage location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeltaCreateStagingTableRequest {
    /// Table name within the path catalog and schema.
    pub name: String,
}

/// Staging allocation returned before the initial Delta commit is written.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaStagingTableResponse {
    /// Catalog-allocated table UUID.
    pub table_id: String,
    /// Always managed for this endpoint.
    pub table_type: DeltaTableType,
    /// Catalog-allocated storage location.
    pub location: String,
    /// Temporary write credentials.
    pub storage_credentials: Vec<DeltaStorageCredential>,
    /// Minimum protocol the initial commit must contain.
    pub required_protocol: DeltaProtocol,
    /// Optional protocol features supported by the server.
    #[serde(default)]
    pub suggested_protocol: Option<DeltaSuggestedProtocol>,
    /// Properties the initial commit must contain.
    pub required_properties: HashMap<String, Option<String>>,
    /// Optional properties suggested by the catalog.
    #[serde(default)]
    pub suggested_properties: HashMap<String, Option<String>>,
}

/// Suggested protocol features that are not mandatory for creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaSuggestedProtocol {
    /// Suggested reader features.
    #[serde(default)]
    pub reader_features: Vec<String>,
    /// Suggested writer features.
    #[serde(default)]
    pub writer_features: Vec<String>,
}

/// Finalize a table whose version-zero log was written at a staging location.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeltaCreateTableRequest {
    /// Table name.
    pub name: String,
    /// Staging location returned by Unity Catalog.
    pub location: String,
    /// Table classification.
    pub table_type: DeltaTableType,
    /// Optional table comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Validated Delta schema serialized in the Delta Catalog wire representation.
    pub columns: DeltaStructType,
    /// Logical partition columns.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,
    /// Initial Delta protocol.
    pub protocol: DeltaProtocol,
    /// Initial Delta properties.
    pub properties: HashMap<String, String>,
    /// Version-zero in-commit timestamp.
    pub last_commit_timestamp_ms: i64,
}

/// A validated kernel schema viewed through the Delta Catalog wire representation.
///
/// Delta kernel retains the logical schema and validation rules. This wrapper changes only the
/// JSON member names for complex types from Delta log camelCase to the catalog API's kebab-case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaStructType(KernelStructType);

impl From<KernelStructType> for DeltaStructType {
    fn from(schema: KernelStructType) -> Self {
        Self(schema)
    }
}

impl Serialize for DeltaStructType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DeltaStructTypeWire(&self.0).serialize(serializer)
    }
}

struct DeltaStructTypeWire<'a>(&'a KernelStructType);

impl Serialize for DeltaStructTypeWire<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DeltaStructTypeObject {
            type_name: "struct",
            fields: self.0.fields().map(DeltaStructFieldWire::from).collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Serialize)]
struct DeltaStructTypeObject<'a> {
    #[serde(rename = "type")]
    type_name: &'static str,
    fields: Vec<DeltaStructFieldWire<'a>>,
}

#[derive(Serialize)]
struct DeltaStructFieldWire<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    data_type: DeltaDataTypeWire<'a>,
    nullable: bool,
    metadata: &'a HashMap<String, MetadataValue>,
}

impl<'a> From<&'a StructField> for DeltaStructFieldWire<'a> {
    fn from(field: &'a StructField) -> Self {
        Self {
            name: &field.name,
            data_type: DeltaDataTypeWire(field.data_type()),
            nullable: field.nullable,
            metadata: &field.metadata,
        }
    }
}

struct DeltaDataTypeWire<'a>(&'a DataType);

impl Serialize for DeltaDataTypeWire<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            DataType::Primitive(_) | DataType::Variant(_) => self.0.serialize(serializer),
            DataType::Array(array) => DeltaArrayTypeWire {
                type_name: "array",
                element_type: DeltaDataTypeWire(array.element_type()),
                contains_null: array.contains_null(),
            }
            .serialize(serializer),
            DataType::Struct(schema) => DeltaStructTypeWire(schema).serialize(serializer),
            DataType::Map(map) => DeltaMapTypeWire {
                type_name: "map",
                key_type: DeltaDataTypeWire(map.key_type()),
                value_type: DeltaDataTypeWire(map.value_type()),
                value_contains_null: map.value_contains_null(),
            }
            .serialize(serializer),
        }
    }
}

#[derive(Serialize)]
struct DeltaArrayTypeWire<'a> {
    #[serde(rename = "type")]
    type_name: &'static str,
    #[serde(rename = "element-type")]
    element_type: DeltaDataTypeWire<'a>,
    #[serde(rename = "contains-null")]
    contains_null: bool,
}

#[derive(Serialize)]
struct DeltaMapTypeWire<'a> {
    #[serde(rename = "type")]
    type_name: &'static str,
    #[serde(rename = "key-type")]
    key_type: DeltaDataTypeWire<'a>,
    #[serde(rename = "value-type")]
    value_type: DeltaDataTypeWire<'a>,
    #[serde(rename = "value-contains-null")]
    value_contains_null: bool,
}

/// Optimistic precondition attached to a table update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum DeltaTableRequirement {
    /// Require the stable catalog identity.
    #[serde(rename = "assert-table-uuid")]
    AssertTableUuid {
        /// Expected table UUID.
        uuid: String,
    },
    /// Require the metadata etag observed while constructing metadata changes.
    #[serde(rename = "assert-etag")]
    AssertEtag {
        /// Expected etag.
        etag: String,
    },
}

/// Supported domain metadata values in the current Unity Catalog wire contract.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeltaDomainMetadataUpdates {
    /// Clustering configuration.
    #[serde(rename = "delta.clustering", skip_serializing_if = "Option::is_none")]
    pub clustering: Option<DeltaClusteringDomainMetadata>,
    /// Row-tracking high-water mark.
    #[serde(rename = "delta.rowTracking", skip_serializing_if = "Option::is_none")]
    pub row_tracking: Option<DeltaRowTrackingDomainMetadata>,
}

/// Clustering domain metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaClusteringDomainMetadata {
    /// Nested physical column paths.
    #[serde(rename = "clusteringColumns")]
    pub clustering_columns: Vec<Vec<String>>,
}

/// Row-tracking domain metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaRowTrackingDomainMetadata {
    /// Highest allocated row identifier.
    #[serde(rename = "rowIdHighWaterMark")]
    pub row_id_high_water_mark: i64,
}

/// Atomic table update action.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action")]
pub enum DeltaTableUpdate {
    /// Set Delta configuration entries.
    #[serde(rename = "set-properties")]
    SetProperties {
        /// Entries to set.
        updates: HashMap<String, String>,
    },
    /// Remove Delta configuration entries.
    #[serde(rename = "remove-properties")]
    RemoveProperties {
        /// Keys to remove.
        removals: Vec<String>,
    },
    /// Replace the schema.
    #[serde(rename = "set-columns")]
    SetColumns {
        /// Validated Delta schema in the Delta Catalog wire representation.
        columns: DeltaStructType,
    },
    /// Replace the table comment.
    #[serde(rename = "set-table-comment")]
    SetTableComment {
        /// New comment.
        comment: String,
    },
    /// Ratify one staged commit.
    #[serde(rename = "add-commit")]
    AddCommit {
        /// Staged commit metadata.
        commit: DeltaCommit,
    },
    /// Report publication through a version.
    #[serde(rename = "set-latest-backfilled-version")]
    SetLatestBackfilledVersion {
        /// Highest published version.
        #[serde(rename = "latest-published-version")]
        latest_published_version: i64,
    },
    /// Replace the Delta protocol.
    #[serde(rename = "set-protocol")]
    SetProtocol {
        /// New protocol.
        protocol: DeltaProtocol,
    },
    /// Set supported domain metadata.
    #[serde(rename = "set-domain-metadata")]
    SetDomainMetadata {
        /// Domain updates.
        updates: DeltaDomainMetadataUpdates,
    },
    /// Remove supported domain metadata.
    #[serde(rename = "remove-domain-metadata")]
    RemoveDomainMetadata {
        /// Domains to remove.
        domains: Vec<String>,
    },
    /// Replace logical partition columns.
    #[serde(rename = "set-partition-columns")]
    SetPartitionColumns {
        /// New partition columns.
        #[serde(rename = "partition-columns")]
        partition_columns: Vec<String>,
    },
}

/// Atomic request containing update preconditions and actions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeltaUpdateTableRequest {
    /// Preconditions checked against the same catalog transaction.
    pub requirements: Vec<DeltaTableRequirement>,
    /// Actions applied atomically.
    pub updates: Vec<DeltaTableUpdate>,
}

#[derive(Debug, Deserialize)]
struct DeltaErrorResponse {
    error: DeltaErrorModel,
}

#[derive(Debug, Deserialize)]
struct DeltaErrorModel {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    code: u16,
}

impl UnityCatalogError {
    /// Stable Delta API error identifier, when this error came from a structured response.
    pub fn delta_error_type(&self) -> Option<&str> {
        match self {
            Self::DeltaApi { error_type, .. } => Some(error_type),
            _ => None,
        }
    }
}

impl UnityCatalog {
    /// Negotiate the Delta Catalog protocol for one catalog.
    pub async fn delta_catalog_config(
        &self,
        catalog: &str,
    ) -> Result<DeltaCatalogConfig, UnityCatalogError> {
        let mut url = self.delta_api_url(&["config"])?;
        url.query_pairs_mut()
            .append_pair("catalog", catalog)
            .append_pair("protocol-versions", DELTA_PROTOCOL_VERSION);
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .send()
            .await?;
        decode_delta_response(response, "configuration").await
    }

    /// Load catalog metadata and the complete ratified, unbackfilled commit tail atomically.
    pub async fn load_delta_table(
        &self,
        table: &DeltaTableReference,
    ) -> Result<DeltaLoadTableResponse, UnityCatalogError> {
        let url = self.delta_table_url(table, &[])?;
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .send()
            .await?;
        decode_delta_response(response, "load-table").await
    }

    /// Request an exactly-scoped table credential. This never falls back from write to read.
    pub async fn delta_table_credentials(
        &self,
        table: &DeltaTableReference,
        operation: DeltaCredentialOperation,
    ) -> Result<DeltaCredentialsResponse, UnityCatalogError> {
        let mut url = self.delta_table_url(table, &["credentials"])?;
        url.query_pairs_mut()
            .append_pair("operation", operation.as_str());
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .send()
            .await?;
        decode_delta_response(response, "table-credentials").await
    }

    /// Apply catalog requirements and updates atomically.
    pub async fn update_delta_table(
        &self,
        table: &DeltaTableReference,
        request: &DeltaUpdateTableRequest,
    ) -> Result<DeltaLoadTableResponse, UnityCatalogError> {
        let url = self.delta_table_url(table, &[])?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .json(request)
            .send()
            .await?;
        decode_delta_response(response, "update-table").await
    }

    /// Allocate a managed staging table.
    pub async fn create_delta_staging_table(
        &self,
        table: &DeltaTableReference,
    ) -> Result<DeltaStagingTableResponse, UnityCatalogError> {
        let url = self.delta_api_url(&[
            "catalogs",
            table.catalog(),
            "schemas",
            table.schema(),
            "staging-tables",
        ])?;
        let request = DeltaCreateStagingTableRequest {
            name: table.table().to_owned(),
        };
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .json(&request)
            .send()
            .await?;
        decode_delta_response(response, "create-staging-table").await
    }

    /// Re-vend write credentials for an existing staging allocation.
    pub async fn delta_staging_credentials(
        &self,
        table_id: &str,
    ) -> Result<DeltaCredentialsResponse, UnityCatalogError> {
        let url = self.delta_api_url(&["staging-tables", table_id, "credentials"])?;
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .send()
            .await?;
        decode_delta_response(response, "staging-table-credentials").await
    }

    /// Finalize a staged managed table after its initial Delta commit exists.
    pub async fn create_delta_table(
        &self,
        table: &DeltaTableReference,
        request: &DeltaCreateTableRequest,
    ) -> Result<DeltaLoadTableResponse, UnityCatalogError> {
        let url = self.delta_api_url(&[
            "catalogs",
            table.catalog(),
            "schemas",
            table.schema(),
            "tables",
        ])?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.get_credential().await?)
            .json(request)
            .send()
            .await?;
        decode_delta_response(response, "create-table").await
    }

    fn delta_table_url(
        &self,
        table: &DeltaTableReference,
        suffix: &[&str],
    ) -> Result<Url, UnityCatalogError> {
        let mut segments = vec![
            "catalogs",
            table.catalog(),
            "schemas",
            table.schema(),
            "tables",
            table.table(),
        ];
        segments.extend_from_slice(suffix);
        self.delta_api_url(&segments)
    }

    fn delta_api_url(&self, segments: &[&str]) -> Result<Url, UnityCatalogError> {
        let mut url =
            Url::parse(&self.workspace_url).map_err(|source| UnityCatalogError::Generic {
                source: Box::new(source),
            })?;
        url.set_path(DELTA_API_PATH);
        url.set_query(None);
        url.set_fragment(None);
        let mut path = url
            .path_segments_mut()
            .map_err(|_| UnityCatalogError::InitializationError)?;
        path.pop_if_empty();
        path.extend(segments.iter().copied());
        drop(path);
        Ok(url)
    }
}

async fn decode_delta_response<T: DeserializeOwned>(
    response: reqwest::Response,
    response_name: &'static str,
) -> Result<T, UnityCatalogError> {
    let status = response.status();
    let body = response.bytes().await?;
    if status.is_success() {
        return serde_json::from_slice(&body).map_err(|source| {
            UnityCatalogError::InvalidDeltaResponse {
                response: response_name,
                source,
            }
        });
    }

    let error = serde_json::from_slice::<DeltaErrorResponse>(&body).ok();
    Err(UnityCatalogError::DeltaApi {
        status: status.as_u16(),
        error_type: error.as_ref().map_or_else(
            || ERROR_TYPE_UNAVAILABLE.to_owned(),
            |error| error.error.error_type.clone(),
        ),
        message: error.map_or_else(
            || ERROR_MESSAGE_UNAVAILABLE.to_owned(),
            |error| {
                if error.error.code == status.as_u16() {
                    error.error.message
                } else {
                    format!(
                        "{} (body code {}, HTTP status {})",
                        error.error.message,
                        error.error.code,
                        status.as_u16()
                    )
                }
            },
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::kernel::{ArrayType, MapType};

    fn credential(
        prefix: &str,
        operation: DeltaCredentialOperation,
        expiration_time_ms: i64,
    ) -> DeltaStorageCredential {
        DeltaStorageCredential {
            prefix: prefix.to_owned(),
            operation,
            config: HashMap::from([
                ("s3.access-key-id".to_owned(), "access".to_owned()),
                ("s3.secret-access-key".to_owned(), "secret".to_owned()),
                ("s3.session-token".to_owned(), "session".to_owned()),
            ]),
            expiration_time_ms,
        }
    }

    #[test]
    fn table_reference_requires_exactly_three_non_empty_parts() {
        assert!(DeltaTableReference::try_from_uri("uc://catalog.schema.table").is_ok());
        assert!(DeltaTableReference::try_from_uri("uc://catalog.schema.table/").is_ok());
        assert!(DeltaTableReference::try_from_uri("s3://catalog.schema.table").is_err());
        assert!(DeltaTableReference::try_from_uri("uc://catalog.schema").is_err());
        assert!(DeltaTableReference::try_from_uri("uc://catalog..table").is_err());
        assert!(DeltaTableReference::try_from_uri("uc://catalog.schema.table/path").is_err());
        assert!(DeltaTableReference::try_from_uri("uc://catalog.schema.table?query=1").is_err());
    }

    #[test]
    fn catalog_schema_serializes_complex_types_with_kebab_case_members() {
        let schema = KernelStructType::try_new([StructField::new(
            "nested",
            ArrayType::new(MapType::new(DataType::STRING, DataType::LONG, true), false),
            true,
        )])
        .expect("valid nested Delta schema");
        let wire = serde_json::to_value(DeltaStructType::from(schema.clone()))
            .expect("catalog schema is serializable");
        let array = &wire["fields"][0]["type"];
        let map = &array["element-type"];

        assert_eq!(array["type"], "array");
        assert_eq!(array["contains-null"], false);
        assert!(array.get("elementType").is_none());
        assert_eq!(map["type"], "map");
        assert_eq!(map["key-type"], "string");
        assert_eq!(map["value-type"], "long");
        assert_eq!(map["value-contains-null"], true);
        assert!(map.get("valueContainsNull").is_none());

        let update = serde_json::to_value(DeltaTableUpdate::SetColumns {
            columns: schema.into(),
        })
        .expect("catalog schema update is serializable");
        let updated_array = &update["columns"]["fields"][0]["type"];
        assert!(updated_array.get("element-type").is_some());
        assert!(updated_array.get("elementType").is_none());
    }

    #[test]
    fn credentials_are_selected_by_intent_expiry_and_longest_prefix() {
        let now = Utc::now().timestamp_millis();
        let response = DeltaCredentialsResponse {
            storage_credentials: vec![
                credential(
                    "s3://bucket/",
                    DeltaCredentialOperation::ReadWrite,
                    now + 10_000,
                ),
                credential(
                    "s3://bucket/table/",
                    DeltaCredentialOperation::Read,
                    now + 10_000,
                ),
                credential(
                    "s3://bucket/table/",
                    DeltaCredentialOperation::ReadWrite,
                    now - 1,
                ),
                credential(
                    "s3://bucket/table/",
                    DeltaCredentialOperation::ReadWrite,
                    now + 10_000,
                ),
            ],
        };

        let selected = response
            .storage_options_for("s3://bucket/table", DeltaCredentialOperation::ReadWrite)
            .expect("the exact unexpired write credential is selected");
        assert_eq!(selected.prefix(), "s3://bucket/table/");
        assert_eq!(selected.options()["AWS_SESSION_TOKEN"], "session");
    }

    #[test]
    fn protocol_negotiation_fails_closed() {
        let config = DeltaCatalogConfig {
            endpoints: vec![endpoint::LOAD_TABLE.to_owned()],
            protocol_version: DELTA_PROTOCOL_VERSION.to_owned(),
        };
        assert!(config.require(&[endpoint::LOAD_TABLE]).is_ok());
        assert!(config.require(&[endpoint::UPDATE_TABLE]).is_err());

        let unsupported = DeltaCatalogConfig {
            endpoints: vec![endpoint::LOAD_TABLE.to_owned()],
            protocol_version: "2.0".to_owned(),
        };
        assert!(unsupported.require(&[endpoint::LOAD_TABLE]).is_err());
    }
}
