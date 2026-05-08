//! Router trait (RFC 0002 rev 5 §`Router`).
//!
//! The default `StaticRouter` maps `(source, signal_type) -> [routes]`
//! and assigns every record to every route — the v1 path the current
//! ClickHouse logs flow takes. Record-level filtering is reserved
//! for v2 routers.

use std::fmt;
use std::sync::Arc;

use crate::decoded_batch::DecodedBatch;
use crate::error::RuntimeResult;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteId(pub String);

impl fmt::Display for RouteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for RouteId {
    fn from(s: String) -> Self {
        RouteId(s)
    }
}

impl From<&str> for RouteId {
    fn from(s: &str) -> Self {
        RouteId(s.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct RouteAssignment {
    pub route: RouteId,
    /// Indices into the batch's records that go to this route.
    /// `None` means "all records." For v1 the default router emits
    /// one assignment per route covering all records (no
    /// record-level filtering). The `Arc` carrier matches RFC 0002
    /// rev 5 — sinks share the same index list across fanout.
    pub indices: Option<Arc<Vec<u32>>>,
}

pub trait Router: Send + Sync + 'static {
    fn routes(&self) -> &[RouteId];

    fn route(&self, batch: &DecodedBatch) -> RuntimeResult<Vec<RouteAssignment>>;
}

/// Default v1 router: emits one assignment per configured route,
/// each covering every record in the batch.
pub struct StaticRouter {
    routes: Vec<RouteId>,
}

impl StaticRouter {
    pub fn new(routes: Vec<RouteId>) -> Self {
        Self { routes }
    }
}

impl Router for StaticRouter {
    fn routes(&self) -> &[RouteId] {
        &self.routes
    }

    fn route(&self, _batch: &DecodedBatch) -> RuntimeResult<Vec<RouteAssignment>> {
        Ok(self
            .routes
            .iter()
            .cloned()
            .map(|route| RouteAssignment {
                route,
                indices: None,
            })
            .collect())
    }
}
