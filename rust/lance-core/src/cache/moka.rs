// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;

use async_trait::async_trait;
use futures::Future;

use crate::Result;

use super::backend::{CacheBackend, CacheEntry};

/// Internal record stored in the moka cache.
#[derive(Clone, Debug)]
struct MokaCacheEntry {
    entry: CacheEntry,
    size_bytes: usize,
}

/// Default [`CacheBackend`] backed by a [moka](https://crates.io/crates/moka) cache.
///
/// Provides weighted-capacity eviction and concurrent-load deduplication
/// via moka's built-in `optionally_get_with`.
pub struct MokaCacheBackend {
    cache: moka::future::Cache<Vec<u8>, MokaCacheEntry>,
}

impl std::fmt::Debug for MokaCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaCacheBackend")
            .field("entry_count", &self.cache.entry_count())
            .finish()
    }
}

impl MokaCacheBackend {
    pub fn with_capacity(capacity: usize) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(capacity as u64)
            .weigher(|_, v: &MokaCacheEntry| v.size_bytes.try_into().unwrap_or(u32::MAX))
            .support_invalidation_closures()
            .build();
        Self { cache }
    }

    pub fn no_cache() -> Self {
        Self {
            cache: moka::future::Cache::new(0),
        }
    }
}

#[async_trait]
impl CacheBackend for MokaCacheBackend {
    async fn get(&self, key: &[u8]) -> Option<CacheEntry> {
        self.cache.get(key).await.map(|r| r.entry)
    }

    async fn insert(&self, key: &[u8], entry: CacheEntry, size_bytes: usize) {
        self.cache
            .insert(key.to_vec(), MokaCacheEntry { entry, size_bytes })
            .await;
    }

    async fn get_or_insert<'a>(
        &self,
        key: &[u8],
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
    ) -> Result<CacheEntry> {
        // Use moka's built-in dedup: optionally_get_with runs the init future
        // at most once per key, even under concurrent access.
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();

        let init = async move {
            match loader.await {
                Ok((entry, size_bytes)) => Some(MokaCacheEntry { entry, size_bytes }),
                Err(e) => {
                    let _ = error_tx.send(e);
                    None
                }
            }
        };

        let owned_key = key.to_vec();
        match self.cache.optionally_get_with(owned_key, init).await {
            Some(record) => Ok(record.entry),
            None => match error_rx.await {
                Ok(err) => Err(err),
                Err(_) => Err(crate::Error::internal(
                    "Failed to retrieve error from cache loader",
                )),
            },
        }
    }

    async fn invalidate_prefix(&self, prefix: &[u8]) {
        let prefix = prefix.to_vec();
        self.cache
            .invalidate_entries_if(move |key, _value| key.starts_with(&prefix))
            .expect("Cache configured correctly");
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
    }

    async fn num_entries(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count() as usize
    }

    async fn size_bytes(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size() as usize
    }

    fn approx_num_entries(&self) -> usize {
        self.cache.entry_count() as usize
    }

    fn approx_size_bytes(&self) -> usize {
        self.cache.iter().map(|(_, v)| v.size_bytes).sum()
    }
}
