//! Resolve the S3 missing-object ambiguity only for Delta's optional checkpoint hint.
//!
//! Prefix-scoped ListBucket policies can make a missing GET return AccessDenied.
//! Never reinterpret that status alone: the same scoped store must successfully
//! finish listing the hint's parent and prove that the exact object is absent.
//! Existing objects, list failures, and all non-hint reads retain the original error.

use futures::{TryStreamExt, stream::BoxStream};
use object_store::{Error, ObjectMeta, Result, path::Path};

const CHECKPOINT_HINT_NAME: &str = "/_last_checkpoint";
const DELTA_LOG_DIRECTORY: &str = "_delta_log";
const NESTED_DELTA_LOG_DIRECTORY: &str = "/_delta_log";

pub(crate) type ScopedListing<'a> =
    dyn Fn(Path) -> BoxStream<'static, Result<ObjectMeta>> + Send + Sync + 'a;

pub(crate) async fn resolve_denied_hint(
    location: &Path,
    error: Error,
    list: &ScopedListing<'_>,
) -> Error {
    let path = location.as_ref();
    if !matches!(error, Error::PermissionDenied { .. }) {
        return error;
    }
    let Some(parent) = path.strip_suffix(CHECKPOINT_HINT_NAME).filter(|parent| {
        *parent == DELTA_LOG_DIRECTORY || parent.ends_with(NESTED_DELTA_LOG_DIRECTORY)
    }) else {
        return error;
    };
    let mut objects = list(Path::from(parent));
    loop {
        match objects.try_next().await {
            Ok(Some(object)) if object.location == *location => return error,
            Ok(Some(_)) => {}
            Ok(None) => {
                return Error::NotFound {
                    path: location.to_string(),
                    source: Box::new(error),
                };
            }
            Err(_) => return error,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::StreamExt;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;

    const CHECKPOINT_HINT_SUFFIX: &str = "_delta_log/_last_checkpoint";

    fn denied(path: &Path) -> Error {
        Error::PermissionDenied {
            path: path.to_string(),
            source: "fixture read denial".into(),
        }
    }

    #[tokio::test]
    async fn only_exact_optional_hint_denials_can_list() {
        let calls = AtomicUsize::new(0);
        for name in [
            "_delta_log/00000000000000000000.json",
            "table/part.parquet",
            "table/_last_checkpoint",
            "table/not_delta_log/_last_checkpoint",
            "table/_delta_log/_last_checkpoint.extra",
        ] {
            let path = Path::from(name);
            let error = resolve_denied_hint(&path, denied(&path), &|_| {
                calls.fetch_add(1, Ordering::SeqCst);
                futures::stream::empty().boxed()
            })
            .await;
            assert!(matches!(error, Error::PermissionDenied { .. }));
        }
        for error in [
            Error::NotFound {
                path: "hint".into(),
                source: "missing".into(),
            },
            Error::Generic {
                store: "fixture",
                source: "network failure".into(),
            },
        ] {
            let path = Path::from(CHECKPOINT_HINT_SUFFIX);
            let actual = resolve_denied_hint(&path, error, &|_| {
                calls.fetch_add(1, Ordering::SeqCst);
                futures::stream::empty().boxed()
            })
            .await;
            assert!(!matches!(actual, Error::PermissionDenied { .. }));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn successful_scoped_listing_must_prove_exact_absence() -> Result<()> {
        for root in ["", "tenant/table/"] {
            let store = Arc::new(InMemory::new());
            let hint = Path::from(format!("{root}{CHECKPOINT_HINT_SUFFIX}"));
            store
                .put(
                    &Path::from(format!("{root}_delta_log/00000000000000000000.json")),
                    "log".into(),
                )
                .await?;
            // An adjacent name is not evidence that the exact hint exists.
            store
                .put(
                    &Path::from(format!("{root}{CHECKPOINT_HINT_SUFFIX}.extra")),
                    "other".into(),
                )
                .await?;
            let missing = resolve_denied_hint(&hint, denied(&hint), &|prefix| {
                assert_eq!(prefix, Path::from(format!("{root}_delta_log")));
                store.list(Some(&prefix))
            })
            .await;
            assert!(matches!(missing, Error::NotFound { .. }));
            store.put(&hint, "existing but denied".into()).await?;
            let exists =
                resolve_denied_hint(&hint, denied(&hint), &|prefix| store.list(Some(&prefix)))
                    .await;
            assert!(matches!(exists, Error::PermissionDenied { .. }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn denied_unavailable_and_partially_failed_listings_never_clear_a_denial() {
        let hint = Path::from(format!("tenant/table/{CHECKPOINT_HINT_SUFFIX}"));
        let factories: [fn(&Path) -> Error; 2] = [denied, |_| Error::Generic {
            store: "fixture",
            source: "unavailable".into(),
        }];
        for make_error in factories {
            let error = resolve_denied_hint(&hint, denied(&hint), &|_| {
                futures::stream::iter([Err(make_error(&hint))]).boxed()
            })
            .await;
            assert!(matches!(error, Error::PermissionDenied { .. }));
        }
        let other = ObjectMeta {
            location: Path::from("tenant/table/_delta_log/00000000000000000000.json"),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        };
        let error = resolve_denied_hint(&hint, denied(&hint), &|_| {
            futures::stream::iter([Ok(other.clone()), Err(denied(&hint))]).boxed()
        })
        .await;
        assert!(matches!(error, Error::PermissionDenied { .. }));
    }
}
