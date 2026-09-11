//! Explicit, credential-free routing for catalog-returned S3 locations.
//! Route selection happens after native catalog resolution and before object
//! storage construction, including managed-table staging. No metadata preflight
//! or process-global credential fallback is involved.

use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Non-secret native storage option containing an encoded route profile.
pub const UNITY_STORAGE_ROUTES_KEY: &str = "unity_storage_routes";

/// A native transport profile for one object-store root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StorageRoute {
    root: Url,
    endpoint: Url,
    region: String,
    virtual_hosted: bool,
}
/// Route validation failures contain no locations or credentials.
#[derive(Debug, thiserror::Error)]
pub enum StorageRouteError {
    #[error("invalid catalog storage root")]
    Root,
    #[error("invalid HTTPS object-store endpoint")]
    Endpoint,
    #[error("missing object-store region")]
    Region,
    #[error("overlapping catalog storage routes")]
    Overlap,
    #[error("no approved transport for catalog storage root")]
    Missing,
    #[error("invalid catalog storage route configuration")]
    Encoding,
}
impl S3StorageRoute {
    /// Validate a non-secret transport and physical root, not a data grant.
    pub fn new(
        root: Url,
        endpoint: Url,
        region: String,
        virtual_hosted: bool,
    ) -> Result<Self, StorageRouteError> {
        let route = Self {
            root,
            endpoint,
            region,
            virtual_hosted,
        };
        route.validate()?;
        Ok(route)
    }
    fn validate(&self) -> Result<(), StorageRouteError> {
        validate_root(&self.root)?;
        if self.endpoint.scheme() != "https"
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
            || self.endpoint.path() != "/"
        {
            return Err(StorageRouteError::Endpoint);
        }
        if self.region.is_empty() || self.region.trim() != self.region {
            return Err(StorageRouteError::Region);
        }
        Ok(())
    }
    /// The profile's non-secret native object_store options.
    pub fn options(&self) -> HashMap<String, String> {
        HashMap::from([
            ("AWS_ENDPOINT_URL".into(), self.endpoint.to_string()),
            ("AWS_REGION".into(), self.region.clone()),
            (
                "AWS_S3_ADDRESSING_STYLE".into(),
                if self.virtual_hosted {
                    "virtual"
                } else {
                    "path"
                }
                .into(),
            ),
        ])
    }
    /// Native HTTP endpoint, never inferred from a source declaration.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }
    /// Signature region.
    pub fn region(&self) -> &str {
        &self.region
    }
    /// Whether the native store addresses buckets by host.
    pub fn virtual_hosted(&self) -> bool {
        self.virtual_hosted
    }
}

/// Immutable routing rules, validated both at construction and deserialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CatalogStorageRoutes(Vec<S3StorageRoute>);
impl CatalogStorageRoutes {
    /// Reject ambiguous roots even if their transport happens to match today.
    pub fn new(routes: Vec<S3StorageRoute>) -> Result<Self, StorageRouteError> {
        for (index, route) in routes.iter().enumerate() {
            route.validate()?;
            if routes[..index].iter().any(|other| {
                contains(&other.root, &route.root) || contains(&route.root, &other.root)
            }) {
                return Err(StorageRouteError::Overlap);
            }
        }
        Ok(Self(routes))
    }
    /// Add one explicitly configured root.
    pub fn with(self, route: S3StorageRoute) -> Result<Self, StorageRouteError> {
        let mut routes = self.0;
        routes.push(route);
        Self::new(routes)
    }
    /// Resolve exactly one configured prefix using URL-component boundaries.
    pub fn for_location(&self, location: &Url) -> Result<&S3StorageRoute, StorageRouteError> {
        validate_root(location)?;
        self.0
            .iter()
            .find(|route| contains(&route.root, location))
            .ok_or(StorageRouteError::Missing)
    }
    /// Native storage-option encoding. Contains no credential or data.
    pub fn encode(&self) -> Result<String, StorageRouteError> {
        serde_json::to_string(self).map_err(|_| StorageRouteError::Encoding)
    }
    /// Decode and revalidate operator configuration at the native I/O boundary.
    pub fn decode(value: &str) -> Result<Self, StorageRouteError> {
        let routes: Vec<S3StorageRoute> =
            serde_json::from_str(value).map_err(|_| StorageRouteError::Encoding)?;
        Self::new(routes)
    }
}
fn validate_root(root: &Url) -> Result<(), StorageRouteError> {
    if root.scheme() != "s3"
        || root.host_str().is_none()
        || !root.username().is_empty()
        || root.password().is_some()
        || root.query().is_some()
        || root.fragment().is_some()
        || root.port().is_some()
        || root.path().contains('%')
        || root.path().contains('\\')
    {
        return Err(StorageRouteError::Root);
    }
    Ok(())
}
fn contains(root: &Url, location: &Url) -> bool {
    root.scheme() == location.scheme()
        && root.host_str() == location.host_str()
        && (root.path().trim_end_matches('/') == location.path().trim_end_matches('/')
            || location
                .path()
                .starts_with(&format!("{}/", root.path().trim_end_matches('/'))))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn route(root: &str) -> S3StorageRoute {
        S3StorageRoute::new(
            Url::parse(root).unwrap(),
            Url::parse("https://ceph.example").unwrap(),
            "region".into(),
            false,
        )
        .unwrap()
    }
    #[test]
    fn prefix_is_not_a_string_prefix_and_missing_or_ambiguous_routes_fail_closed() {
        let routes = CatalogStorageRoutes::new(vec![route("s3://tenant/a")]).unwrap();
        for location in [
            "s3://tenant/a",
            "s3://tenant/a/file",
            "s3://tenant/a/_staging/id",
        ] {
            assert!(routes.for_location(&Url::parse(location).unwrap()).is_ok());
        }
        for location in [
            "s3://tenant/ab/file",
            "s3://other/a/file",
            "s3://tenant/a%2fb/file",
        ] {
            assert!(routes.for_location(&Url::parse(location).unwrap()).is_err());
        }
        assert!(
            CatalogStorageRoutes::new(vec![route("s3://tenant/a"), route("s3://tenant/a/b")])
                .is_err()
        );
        assert_eq!(
            CatalogStorageRoutes::decode(&routes.encode().unwrap()).unwrap(),
            routes
        );
        assert!(CatalogStorageRoutes::decode(r#"[{"root":"s3://b/","endpoint":"http://ceph/","region":"x","virtual_hosted":false}]"#).is_err());
    }
}
