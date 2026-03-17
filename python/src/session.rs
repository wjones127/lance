// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow::pyarrow::PyArrowType;
use arrow_array::RecordBatchReader;
use pyo3::{
    Bound, PyResult, Python, pyclass, pymethods,
    types::{PyDict, PyDictMethods},
};

use lance::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use lance::session::Session as LanceSession;

use crate::rt;

/// The Session holds stateful information for a dataset.
///
/// The session contains caches for opened indices and file metadata.
#[pyclass(name = "_Session", module = "_lib")]
#[derive(Clone)]
pub struct Session {
    pub inner: Arc<LanceSession>,
}

impl Session {
    pub fn new(inner: Arc<LanceSession>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl Session {
    #[new]
    #[pyo3(signature=(index_cache_size_bytes=None, metadata_cache_size_bytes=None))]
    fn create(
        index_cache_size_bytes: Option<usize>,
        metadata_cache_size_bytes: Option<usize>,
    ) -> Self {
        let session = LanceSession::new(
            index_cache_size_bytes.unwrap_or(DEFAULT_INDEX_CACHE_SIZE),
            metadata_cache_size_bytes.unwrap_or(DEFAULT_METADATA_CACHE_SIZE),
            Default::default(),
        );
        Self {
            inner: Arc::new(session),
        }
    }

    fn __repr__(&self) -> String {
        let (index_cache_size, meta_cache_size) = rt()
            .block_on(None, async move {
                (
                    self.inner.index_cache_stats().await.size_bytes,
                    self.inner.metadata_cache_stats().await.size_bytes,
                )
            })
            .unwrap_or((0, 0));
        format!(
            "Session(index_cache_size_bytes={}, metadata_cache_size_bytes={})",
            index_cache_size, meta_cache_size
        )
    }

    /// Return the current size of the session in bytes
    pub fn size_bytes(&self) -> u64 {
        self.inner.size_bytes()
    }

    /// Return whether the other session is the same as this one.
    pub fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Return a list of (key, size_bytes) tuples for each entry in the index cache.
    pub fn index_cache_entries(&self) -> Vec<(String, usize)> {
        let inner = self.inner.clone();
        rt().block_on(None, inner.index_cache_entries())
            .unwrap_or_default()
    }

    /// Return cache statistics for the index cache.
    pub fn index_cache_stats<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let inner = self.inner.clone();
        let stats = rt()
            .block_on(None, inner.index_cache_stats())
            .expect("Failed to get index cache stats");
        let dict = PyDict::new(py);
        dict.set_item("hits", stats.hits).unwrap();
        dict.set_item("misses", stats.misses).unwrap();
        dict.set_item("num_entries", stats.num_entries).unwrap();
        dict.set_item("size_bytes", stats.size_bytes).unwrap();
        dict
    }

    /// Return cache statistics for the metadata cache.
    pub fn metadata_cache_stats<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        let inner = self.inner.clone();
        let stats = rt()
            .block_on(None, inner.metadata_cache_stats())
            .expect("Failed to get metadata cache stats");
        let dict = PyDict::new(py);
        dict.set_item("hits", stats.hits).unwrap();
        dict.set_item("misses", stats.misses).unwrap();
        dict.set_item("num_entries", stats.num_entries).unwrap();
        dict.set_item("size_bytes", stats.size_bytes).unwrap();
        dict
    }

    /// Return a record batch reader for debug information about the index cache.
    ///
    /// This returns a reader that yields record batches with columns:
    /// - key: str - the cache key
    /// - type_name: str - the type name of the cached value
    /// - size_bytes: uint64 - the size measured independently with a fresh context
    /// - incremental_size_bytes: uint64 - the size measured with shared allocation deduplication
    ///
    /// The difference between size_bytes and incremental_size_bytes shows how much
    /// memory is shared via Arc pointers.
    pub fn debug_index_cache(&self) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let reader = self.inner.debug_index_cache();
        Ok(PyArrowType(reader))
    }
}
