//! Path-filtered counting wrapper for `ObjectStore`.
//!
//! Used by the K>1 admission amortization test: counts GETs against
//! the manifest path separately from all other GETs so the test can
//! assert "8 descriptors admitted in one cycle → 1 manifest GET, not
//! 8" at the S3-trait boundary (not just at the runtime metric
//! layer).
//!
//! Every method delegates to an inner `Arc<dyn ObjectStore>`; only
//! `get_opts` increments a counter, and only that counter splits on
//! path. Producer-side activity (manifest writes via `put_opts`) is
//! NOT counted — callers should reset the counter to zero after
//! producing test fixtures to isolate the runtime's manifest-GET
//! count.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};

pub struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    manifest_path: String,
    manifest_gets: Arc<AtomicU64>,
    data_gets: Arc<AtomicU64>,
}

impl CountingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, manifest_path: impl Into<String>) -> Self {
        Self {
            inner,
            manifest_path: manifest_path.into(),
            manifest_gets: Arc::new(AtomicU64::new(0)),
            data_gets: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn manifest_gets_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.manifest_gets)
    }

    pub fn data_gets_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.data_gets)
    }
}

impl std::fmt::Debug for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingObjectStore")
            .field("manifest_path", &self.manifest_path)
            .field("manifest_gets", &self.manifest_gets.load(Ordering::SeqCst))
            .field("data_gets", &self.data_gets.load(Ordering::SeqCst))
            .finish()
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.manifest_path)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if location.as_ref() == self.manifest_path.as_str() {
            self.manifest_gets.fetch_add(1, Ordering::SeqCst);
        } else {
            self.data_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> OsResult<Bytes> {
        // get_range default impl calls get_opts, but some stores
        // override; route through our get_opts so the counter sees
        // any path that reaches the inner store.
        let options = GetOptions {
            range: Some(range.into()),
            ..Default::default()
        };
        self.get_opts(location, options).await?.bytes().await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        // head is "is this object there" metadata-only and does not
        // count as a GET for the amortization assertion. Delegate
        // directly so we don't inflate the manifest counter when the
        // buffer probes existence.
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
