use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry as DataFusionObjectStoreRegistry, ObjectStoreRegistry,
};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::registry::{
    DefaultObjectStoreRegistry as PathObjectStoreRegistry,
    ObjectStoreRegistry as PathObjectStoreRegistryExt,
};
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result as ObjectStoreResult,
};
use thiserror::Error;
use url::Url;

const ROUTING_ORIGIN: &str = "delta-scoped://registry/";
const MAXIMUM_CONCURRENT_DELETES: usize = 10;
const ROUTING_STORE_NAME: &str = "DeltaScopedObjectStore";

#[derive(Debug, Error)]
enum ScopedObjectStoreError {
    #[error("No table-scoped object store is registered for {operation} at {path}.")]
    Unregistered {
        operation: &'static str,
        path: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error(
        "Object-store {operation} cannot cross table credential scopes from {source_path} to {destination_path}."
    )]
    CrossScope {
        operation: &'static str,
        source_path: String,
        destination_path: String,
    },
    #[error("The object path for {operation} could not be represented as a routing URL: {path}.")]
    InvalidRoutingPath {
        operation: &'static str,
        path: String,
    },
}

fn object_store_error(error: ScopedObjectStoreError) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: ROUTING_STORE_NAME,
        source: Box::new(error),
    }
}

fn origin_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort]
    )
}

struct ResolvedTableScope {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

#[derive(Debug, Clone)]
struct TableScopedObjectStore {
    origin: String,
    stores: Arc<PathObjectStoreRegistry>,
    routing_origin: Url,
}

impl TableScopedObjectStore {
    fn new(origin: String) -> Self {
        Self {
            origin,
            stores: Arc::new(PathObjectStoreRegistry::new()),
            routing_origin: Url::parse(ROUTING_ORIGIN)
                .expect("The table-scoped object-store routing origin must be a valid URL."),
        }
    }

    fn routing_url(&self, operation: &'static str, path: &Path) -> ObjectStoreResult<Url> {
        let mut url = self.routing_origin.clone();
        let mut segments = url.path_segments_mut().map_err(|()| {
            object_store_error(ScopedObjectStoreError::InvalidRoutingPath {
                operation,
                path: path.to_string(),
            })
        })?;
        segments.clear();
        for part in path.parts() {
            segments.push(part.as_ref());
        }
        drop(segments);
        Ok(url)
    }

    fn register(&self, url: &Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>> {
        let Ok(prefix) = Path::from_url_path(url.path()) else {
            return None;
        };
        let Ok(routing_url) = self.routing_url("register", &prefix) else {
            return None;
        };
        self.stores.register(routing_url, store)
    }

    fn resolve(
        &self,
        operation: &'static str,
        location: &Path,
    ) -> ObjectStoreResult<ResolvedTableScope> {
        let routing_url = self.routing_url(operation, location)?;
        let (store, suffix) = self.stores.resolve(&routing_url).map_err(|source| {
            object_store_error(ScopedObjectStoreError::Unregistered {
                operation,
                path: location.to_string(),
                source,
            })
        })?;
        let prefix_depth = location
            .parts_count()
            .checked_sub(suffix.parts_count())
            .ok_or_else(|| {
                object_store_error(ScopedObjectStoreError::InvalidRoutingPath {
                    operation,
                    path: location.to_string(),
                })
            })?;
        let prefix = location.parts().take(prefix_depth).collect();
        Ok(ResolvedTableScope { store, prefix })
    }

    fn resolve_pair(
        &self,
        operation: &'static str,
        source_path: &Path,
        destination_path: &Path,
    ) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        let source = self.resolve(operation, source_path)?;
        let destination = self.resolve(operation, destination_path)?;
        if source.prefix != destination.prefix {
            return Err(object_store_error(ScopedObjectStoreError::CrossScope {
                operation,
                source_path: source_path.to_string(),
                destination_path: destination_path.to_string(),
            }));
        }
        Ok(source.store)
    }
}

impl Display for TableScopedObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TableScopedObjectStore({})", self.origin)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for TableScopedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let scope = self.resolve("put", location)?;
        scope.store.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let scope = self.resolve("put_multipart", location)?;
        scope.store.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let scope = self.resolve("get", location)?;
        scope.store.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        let scope = self.resolve("get_ranges", location)?;
        scope.store.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let router = self.clone();
        locations
            .map(move |location| {
                let router = router.clone();
                async move {
                    let location = location?;
                    let scope = router.resolve("delete", &location)?;
                    scope.store.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(MAXIMUM_CONCURRENT_DELETES)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let root = Path::ROOT;
        let routing_path = prefix.unwrap_or(&root);
        match self.resolve("list", routing_path) {
            Ok(scope) => scope.store.list(prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let root = Path::ROOT;
        let routing_path = prefix.unwrap_or(&root);
        let scope = self.resolve("list_with_delimiter", routing_path)?;
        scope.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let store = self.resolve_pair("copy", from, to)?;
        store.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        let store = self.resolve_pair("rename", from, to)?;
        store.rename_opts(from, to, options).await
    }
}

#[derive(Debug, Default)]
pub(super) struct ScopedObjectStoreRegistry {
    fallback: DataFusionObjectStoreRegistry,
    stores: DashMap<String, Arc<TableScopedObjectStore>>,
}

impl ObjectStoreRegistry for ScopedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let origin = origin_key(url);
        let router: Arc<TableScopedObjectStore> = {
            let entry = self
                .stores
                .entry(origin.clone())
                .or_insert_with(|| Arc::new(TableScopedObjectStore::new(origin)));
            Arc::clone(entry.value())
        };
        router.register(url, store)
    }

    fn get_store(&self, url: &Url) -> DataFusionResult<Arc<dyn ObjectStore>> {
        let origin = origin_key(url);
        if let Some(router) = self.stores.get(&origin) {
            return Ok(Arc::clone(router.value()) as Arc<dyn ObjectStore>);
        }
        self.fallback.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::object_store::ObjectStoreUrl;
    use object_store::memory::InMemory;

    const FIRST_OBJECT: &[u8] = b"first-table";
    const SECOND_OBJECT: &[u8] = b"second-table";

    #[tokio::test]
    async fn routes_same_origin_objects_to_their_registered_table_stores() {
        let registry = ScopedObjectStoreRegistry::default();
        let first_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let second_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_path = Path::from("tenant/table-one/data.parquet");
        let second_path = Path::from("tenant/table-two/data.parquet");
        first_store
            .put(&first_path, Bytes::from_static(FIRST_OBJECT).into())
            .await
            .unwrap();
        second_store
            .put(&second_path, Bytes::from_static(SECOND_OBJECT).into())
            .await
            .unwrap();

        registry.register_store(
            &Url::parse("s3://bucket/tenant/table-one").unwrap(),
            first_store,
        );
        registry.register_store(
            &Url::parse("s3://bucket/tenant/table-two").unwrap(),
            second_store,
        );

        let origin = ObjectStoreUrl::parse("s3://bucket").unwrap();
        let routed = registry.get_store(origin.as_ref()).unwrap();
        let first = routed
            .get(&first_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let second = routed
            .get(&second_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(first.as_ref(), FIRST_OBJECT);
        assert_eq!(second.as_ref(), SECOND_OBJECT);
    }

    #[tokio::test]
    async fn rejects_unregistered_and_cross_scope_paths() {
        let registry = ScopedObjectStoreRegistry::default();
        let first_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let second_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        registry.register_store(
            &Url::parse("s3://bucket/tenant/table-one").unwrap(),
            first_store,
        );
        registry.register_store(
            &Url::parse("s3://bucket/tenant/table-two").unwrap(),
            second_store,
        );

        let origin = ObjectStoreUrl::parse("s3://bucket").unwrap();
        let routed = registry.get_store(origin.as_ref()).unwrap();
        let unregistered = routed
            .get(&Path::from("tenant/unregistered/data.parquet"))
            .await;
        let cross_scope = routed
            .copy(
                &Path::from("tenant/table-one/data.parquet"),
                &Path::from("tenant/table-two/data.parquet"),
            )
            .await;

        assert!(unregistered.is_err());
        assert!(cross_scope.is_err());
    }
}
