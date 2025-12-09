// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cache implementation

use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use futures::{Future, FutureExt};
use moka::future::Cache;
use snafu::location;

use crate::Result;

pub use deepsize::{Context, DeepSizeOf};

type ArcAny = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub struct SizedRecord {
    record: ArcAny,
    size_accessor: Arc<dyn Fn(&ArcAny) -> usize + Send + Sync>,
    type_name: &'static str,
}

impl std::fmt::Debug for SizedRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SizedRecord")
            .field("record", &self.record)
            .field("type_name", &self.type_name)
            .finish()
    }
}

impl DeepSizeOf for SizedRecord {
    fn deep_size_of_children(&self, _: &mut Context) -> usize {
        (self.size_accessor)(&self.record)
    }
}

impl SizedRecord {
    fn new<T: DeepSizeOf + Send + Sync + 'static>(record: Arc<T>) -> Self {
        // +8 for the size of the Arc pointer itself
        let size_accessor =
            |record: &ArcAny| -> usize { record.downcast_ref::<T>().unwrap().deep_size_of() + 8 };
        Self {
            record,
            size_accessor: Arc::new(size_accessor),
            type_name: std::any::type_name::<T>(),
        }
    }
}

#[derive(Clone)]
pub struct LanceCache {
    cache: Arc<Cache<(String, TypeId), SizedRecord>>,
    prefix: String,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl std::fmt::Debug for LanceCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceCache")
            .field("cache", &self.cache)
            .finish()
    }
}

impl DeepSizeOf for LanceCache {
    fn deep_size_of_children(&self, _: &mut Context) -> usize {
        self.cache
            .iter()
            .map(|(_, v)| (v.size_accessor)(&v.record))
            .sum()
    }
}

impl LanceCache {
    pub fn with_capacity(capacity: usize) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity as u64)
            .weigher(|_, v: &SizedRecord| {
                (v.size_accessor)(&v.record).try_into().unwrap_or(u32::MAX)
            })
            .support_invalidation_closures()
            .build();
        Self {
            cache: Arc::new(cache),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn no_cache() -> Self {
        Self {
            cache: Arc::new(Cache::new(0)),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Appends a prefix to the cache key
    ///
    /// If this cache already has a prefix, the new prefix will be appended to
    /// the existing one.
    ///
    /// Prefixes are used to create a namespace for the cache keys to avoid
    /// collisions between different caches.
    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            cache: self.cache.clone(),
            prefix: format!("{}{}/", self.prefix, prefix),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }

    fn get_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    /// Invalidate all entries in the cache that start with the given prefix
    ///
    /// The given prefix is appended to the existing prefix of the cache. If you
    /// want to invalidate all at the current prefix, pass an empty string.
    pub fn invalidate_prefix(&self, prefix: &str) {
        let full_prefix = format!("{}{}", self.prefix, prefix);
        self.cache
            .invalidate_entries_if(move |(key, _typeid), _value| key.starts_with(&full_prefix))
            .expect("Cache configured correctly");
    }

    pub async fn size(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count() as usize
    }

    pub fn approx_size(&self) -> usize {
        self.cache.entry_count() as usize
    }

    pub async fn size_bytes(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.approx_size_bytes()
    }

    pub fn approx_size_bytes(&self) -> usize {
        self.cache.weighted_size() as usize
    }

    async fn insert<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str, metadata: Arc<T>) {
        let key = self.get_key(key);
        let record = SizedRecord::new(metadata);
        tracing::trace!(
            target: "lance_cache::insert",
            key = key,
            type_id = std::any::type_name::<T>(),
            size = (record.size_accessor)(&record.record),
        );
        self.cache.insert((key, TypeId::of::<T>()), record).await;
    }

    pub async fn insert_unsized<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
        metadata: Arc<T>,
    ) {
        // In order to make the data Sized, we wrap in another pointer.
        self.insert(key, Arc::new(metadata)).await
    }

    async fn get<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let key = self.get_key(key);
        if let Some(metadata) = self.cache.get(&(key, TypeId::of::<T>())).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(metadata.record.clone().downcast::<T>().unwrap())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub async fn get_unsized<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
    ) -> Option<Arc<T>> {
        let outer = self.get::<Arc<T>>(key).await?;
        Some(outer.as_ref().clone())
    }

    /// Get an item
    ///
    /// If it exists in the cache return that
    ///
    /// If it doesn't then run `loader` to load the item, insert into cache, and return
    async fn get_or_insert<T: DeepSizeOf + Send + Sync + 'static, F, Fut>(
        &self,
        key: String,
        loader: F,
    ) -> Result<Arc<T>>
    where
        F: FnOnce(&str) -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        let full_key = self.get_key(&key);
        let cache_key = (full_key, TypeId::of::<T>());

        // Use optionally_get_with to handle concurrent requests
        let hits = self.hits.clone();
        let misses = self.misses.clone();

        // Use oneshot channels to track both errors and whether init was run
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();
        let (init_run_tx, mut init_run_rx) = tokio::sync::oneshot::channel();

        let init = Box::pin(async move {
            let _ = init_run_tx.send(());
            misses.fetch_add(1, Ordering::Relaxed);
            match loader(&key).await {
                Ok(value) => Some(SizedRecord::new(Arc::new(value))),
                Err(e) => {
                    let _ = error_tx.send(e);
                    None
                }
            }
        });

        match self.cache.optionally_get_with(cache_key, init).await {
            Some(metadata) => {
                // Check if init was run or if this was a cache hit
                match init_run_rx.try_recv() {
                    Ok(()) => {
                        // Init was run, miss was already recorded
                    }
                    Err(_) => {
                        // Init was not run, this is a cache hit
                        hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(metadata.record.clone().downcast::<T>().unwrap())
            }
            None => {
                // The loader returned an error, retrieve it from the channel
                match error_rx.await {
                    Ok(err) => Err(err),
                    Err(_) => Err(crate::Error::Internal {
                        message: "Failed to retrieve error from cache loader".into(),
                        location: location!(),
                    }),
                }
            }
        }
    }

    pub async fn stats(&self) -> CacheStats {
        self.cache.run_pending_tasks().await;
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            num_entries: self.cache.entry_count() as usize,
            size_bytes: self.cache.weighted_size() as usize,
        }
    }

    pub async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    /// Debug cache size estimates using Archimedes' Method.
    ///
    /// This method drops cache entries one by one in random order, measuring
    /// actual memory usage before and after each drop to compare with `DeepSizeOf`
    /// estimates.
    ///
    /// **Warning**: This method clears the cache! It is intended for debugging
    /// only and should not be used in production.
    ///
    /// The random order helps identify if certain entry types consistently
    /// under/over-estimate their sizes, regardless of drop order effects.
    ///
    /// # Arguments
    ///
    /// * `seed` - Optional random seed for reproducible debugging sessions.
    ///
    /// # Returns
    ///
    /// A [`CacheDebugReport`] containing per-entry measurements and totals.
    #[cfg(feature = "debug-cache")]
    pub async fn debug_sizes(&self, seed: Option<u64>) -> CacheDebugReport {
        use memory_stats::memory_stats;
        use rand::prelude::SliceRandom;
        use rand::SeedableRng;

        // Ensure all pending operations are complete
        self.cache.run_pending_tasks().await;

        // Collect all entries with their keys, type names, and DeepSizeOf sizes
        let mut entries: Vec<((String, TypeId), &'static str, usize)> = self
            .cache
            .iter()
            .map(|(key, record)| {
                let deep_size = (record.size_accessor)(&record.record);
                // key is Arc<(String, TypeId)>, we need to clone the inner tuple
                ((*key).clone(), record.type_name, deep_size)
            })
            .collect();

        // Shuffle entries randomly
        let mut rng = match seed {
            Some(s) => rand::rngs::StdRng::seed_from_u64(s),
            None => rand::rngs::StdRng::from_rng(&mut rand::rng()),
        };
        entries.shuffle(&mut rng);

        let total_deep_size_of: usize = entries.iter().map(|(_, _, size)| size).sum();
        let mut results = Vec::with_capacity(entries.len());
        let mut total_measured: i64 = 0;
        let mut entries_with_measurement: usize = 0;

        for (cache_key, type_name, deep_size) in entries {
            // Measure memory before drop
            let mem_before = memory_stats().map(|s| s.physical_mem);

            // Remove the entry from cache
            self.cache.invalidate(&cache_key).await;

            // Force pending operations and give allocator time to release
            self.cache.run_pending_tasks().await;

            // Small yield to allow memory to be released
            tokio::task::yield_now().await;

            // Measure memory after drop
            let mem_after = memory_stats().map(|s| s.physical_mem);

            let measured = match (mem_before, mem_after) {
                (Some(before), Some(after)) => {
                    let diff = before as i64 - after as i64;
                    total_measured += diff;
                    entries_with_measurement += 1;
                    Some(diff)
                }
                _ => None,
            };

            results.push(CacheDebugEntry {
                key: cache_key.0,
                type_name,
                deep_size_of_bytes: deep_size,
                measured_bytes: measured,
            });
        }

        CacheDebugReport {
            entries: results,
            total_deep_size_of,
            total_measured,
            entries_with_measurement,
        }
    }

    // CacheKey-based methods
    pub async fn insert_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.insert(&cache_key.key(), metadata).boxed().await
    }

    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.get::<K::ValueType>(&cache_key.key()).boxed().await
    }

    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        let key_str = cache_key.key().into_owned();
        Box::pin(self.get_or_insert(key_str, |_| loader())).await
    }

    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.insert_unsized(&cache_key.key(), metadata)
            .boxed()
            .await
    }

    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.get_unsized::<K::ValueType>(&cache_key.key())
            .boxed()
            .await
    }
}

/// A weak reference to a LanceCache, used by indices to avoid circular references.
/// When the original cache is dropped, operations on this will gracefully no-op.
#[derive(Clone, Debug)]
pub struct WeakLanceCache {
    inner: std::sync::Weak<Cache<(String, TypeId), SizedRecord>>,
    prefix: String,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl WeakLanceCache {
    /// Create a weak reference from a strong LanceCache
    pub fn from(cache: &LanceCache) -> Self {
        Self {
            inner: Arc::downgrade(&cache.cache),
            prefix: cache.prefix.clone(),
            hits: cache.hits.clone(),
            misses: cache.misses.clone(),
        }
    }

    /// Appends a prefix to the cache key
    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            inner: self.inner.clone(),
            prefix: format!("{}{}/", self.prefix, prefix),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }

    fn get_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    /// Get an item from cache if the cache is still alive
    pub async fn get<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let cache = self.inner.upgrade()?;
        let key = self.get_key(key);
        if let Some(metadata) = cache.get(&(key, TypeId::of::<T>())).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(metadata.record.clone().downcast::<T>().unwrap())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert an item if the cache is still alive
    /// Returns true if the item was inserted, false if the cache is no longer available
    pub async fn insert<T: DeepSizeOf + Send + Sync + 'static>(
        &self,
        key: &str,
        value: Arc<T>,
    ) -> bool {
        if let Some(cache) = self.inner.upgrade() {
            let key = self.get_key(key);
            let record = SizedRecord::new(value);
            cache.insert((key, TypeId::of::<T>()), record).await;
            true
        } else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert item");
            false
        }
    }

    /// Get or insert an item, computing it if necessary
    pub async fn get_or_insert<T, F, Fut>(&self, key: &str, f: F) -> Result<Arc<T>>
    where
        T: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        if let Some(cache) = self.inner.upgrade() {
            let full_key = self.get_key(key);
            let cache_key = (full_key.clone(), TypeId::of::<T>());

            // Use optionally_get_with to handle concurrent requests properly
            let hits = self.hits.clone();
            let misses = self.misses.clone();

            // Track whether init was run (for metrics)
            let (init_run_tx, mut init_run_rx) = tokio::sync::oneshot::channel();
            let (error_tx, error_rx) = tokio::sync::oneshot::channel();

            let init = Box::pin(async move {
                let _ = init_run_tx.send(());
                misses.fetch_add(1, Ordering::Relaxed);
                match f().await {
                    Ok(value) => Some(SizedRecord::new(Arc::new(value))),
                    Err(e) => {
                        let _ = error_tx.send(e);
                        None
                    }
                }
            });

            match cache.optionally_get_with(cache_key, init).await {
                Some(record) => {
                    // Check if init was run or if this was a cache hit
                    match init_run_rx.try_recv() {
                        Ok(()) => {
                            // Init was run, miss was already recorded
                        }
                        Err(_) => {
                            // Init was not run, this was a cache hit
                            hits.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(record.record.clone().downcast::<T>().unwrap())
                }
                None => {
                    // Init returned None, which means there was an error
                    match error_rx.await {
                        Ok(e) => Err(e),
                        Err(_) => Err(crate::Error::Internal {
                            message: "Failed to receive error from cache init function".to_string(),
                            location: location!(),
                        }),
                    }
                }
            }
        } else {
            log::warn!("WeakLanceCache: cache no longer available, computing without caching");
            f().await.map(Arc::new)
        }
    }

    /// Get or insert an item with a cache key type
    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        let key_str = cache_key.key().into_owned();
        self.get_or_insert(&key_str, loader).await
    }

    /// Insert with a cache key type
    /// Returns true if the item was inserted, false if the cache is no longer available
    pub async fn insert_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>) -> bool
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key_str = cache_key.key().into_owned();
        self.insert(&key_str, value).await
    }

    /// Get with a cache key type
    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key_str = cache_key.key().into_owned();
        self.get(&key_str).await
    }

    /// Get unsized item from cache
    pub async fn get_unsized<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
    ) -> Option<Arc<T>> {
        // For unsized types, we store Arc<T> directly
        let cache = self.inner.upgrade()?;
        let key = self.get_key(key);
        if let Some(metadata) = cache.get(&(key, TypeId::of::<Arc<T>>())).await {
            metadata
                .record
                .clone()
                .downcast::<Arc<T>>()
                .ok()
                .map(|arc| arc.as_ref().clone())
        } else {
            None
        }
    }

    /// Insert unsized item into cache
    pub async fn insert_unsized<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
        value: Arc<T>,
    ) {
        if let Some(cache) = self.inner.upgrade() {
            let key = self.get_key(key);
            let record = SizedRecord::new(Arc::new(value));
            cache.insert((key, TypeId::of::<Arc<T>>()), record).await;
        } else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert unsized item");
        }
    }

    /// Get unsized with a cache key type
    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key_str = cache_key.key();
        self.get_unsized(&key_str).await
    }

    /// Insert unsized with a cache key type
    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key_str = cache_key.key();
        self.insert_unsized(&key_str, value).await
    }
}

pub trait CacheKey {
    type ValueType;

    fn key(&self) -> Cow<'_, str>;
}

pub trait UnsizedCacheKey {
    type ValueType: ?Sized;

    fn key(&self) -> Cow<'_, str>;
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of times `get`, `get_unsized`, or `get_or_insert` found an item in the cache.
    pub hits: u64,
    /// Number of times `get`, `get_unsized`, or `get_or_insert` did not find an item in the cache.
    pub misses: u64,
    /// Number of entries currently in the cache.
    pub num_entries: usize,
    /// Total size in bytes of all entries in the cache.
    pub size_bytes: usize,
}

impl CacheStats {
    pub fn hit_ratio(&self) -> f32 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f32 / (self.hits + self.misses) as f32
        }
    }

    pub fn miss_ratio(&self) -> f32 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.misses as f32 / (self.hits + self.misses) as f32
        }
    }
}

/// Information about a single cache entry during Archimedes' Method debugging.
///
/// See [`LanceCache::debug_sizes`] for details on this debugging approach.
#[cfg(feature = "debug-cache")]
#[derive(Debug, Clone)]
pub struct CacheDebugEntry {
    /// The cache key (string portion)
    pub key: String,
    /// The type name of the cached value (captured via `std::any::type_name`)
    pub type_name: &'static str,
    /// Size reported by `DeepSizeOf`
    pub deep_size_of_bytes: usize,
    /// Actual memory released when this entry was dropped (measured via process memory).
    /// None if memory measurement was unavailable.
    pub measured_bytes: Option<i64>,
}

/// Report from Archimedes' Method cache debugging.
///
/// This report is produced by [`LanceCache::debug_sizes`] and contains measurements
/// comparing estimated sizes (via `DeepSizeOf`) with actual memory released when
/// dropping cache entries.
#[cfg(feature = "debug-cache")]
#[derive(Debug, Clone)]
pub struct CacheDebugReport {
    /// Individual entry measurements
    pub entries: Vec<CacheDebugEntry>,
    /// Sum of all `DeepSizeOf` values
    pub total_deep_size_of: usize,
    /// Sum of all measured memory releases (only entries where measurement succeeded)
    pub total_measured: i64,
    /// Number of entries where memory measurement was available
    pub entries_with_measurement: usize,
}

#[cfg(feature = "debug-cache")]
impl CacheDebugReport {
    /// Calculate the discrepancy between `DeepSizeOf` and measured memory.
    ///
    /// Positive values indicate `DeepSizeOf` underestimates memory usage.
    /// Negative values indicate `DeepSizeOf` overestimates memory usage.
    pub fn discrepancy(&self) -> i64 {
        self.total_measured - self.total_deep_size_of as i64
    }

    /// Calculate the discrepancy as a percentage of `DeepSizeOf`.
    pub fn discrepancy_percent(&self) -> f64 {
        if self.total_deep_size_of == 0 {
            0.0
        } else {
            (self.discrepancy() as f64 / self.total_deep_size_of as f64) * 100.0
        }
    }

    /// Format the report as a human-readable table.
    pub fn format_table(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        writeln!(out, "Cache Debug Report (Archimedes' Method)").unwrap();
        writeln!(out, "========================================").unwrap();
        writeln!(
            out,
            "{:<50} {:<40} {:>15} {:>15}",
            "Key", "Type", "DeepSizeOf", "Measured"
        )
        .unwrap();
        writeln!(out, "{}", "-".repeat(120)).unwrap();

        for entry in &self.entries {
            let measured_str = match entry.measured_bytes {
                Some(m) => format!("{}", m),
                None => "N/A".to_string(),
            };
            // Truncate long strings for display
            let key_display = if entry.key.len() > 47 {
                format!("{}...", &entry.key[..47])
            } else {
                entry.key.clone()
            };
            let type_display = if entry.type_name.len() > 37 {
                format!("{}...", &entry.type_name[..37])
            } else {
                entry.type_name.to_string()
            };
            writeln!(
                out,
                "{:<50} {:<40} {:>15} {:>15}",
                key_display, type_display, entry.deep_size_of_bytes, measured_str
            )
            .unwrap();
        }

        writeln!(out, "{}", "-".repeat(120)).unwrap();
        writeln!(out, "Total DeepSizeOf: {} bytes", self.total_deep_size_of).unwrap();
        writeln!(
            out,
            "Total Measured: {} bytes ({} entries)",
            self.total_measured, self.entries_with_measurement
        )
        .unwrap();
        writeln!(
            out,
            "Discrepancy: {} bytes ({:.1}%)",
            self.discrepancy(),
            self.discrepancy_percent()
        )
        .unwrap();

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_bytes() {
        let item = Arc::new(vec![1, 2, 3]);
        let item_size = item.deep_size_of(); // Size of Arc<Vec<i32>>
        let capacity = 10 * item_size;

        let cache = LanceCache::with_capacity(capacity);
        assert_eq!(cache.size_bytes().await, 0);
        assert_eq!(cache.approx_size_bytes(), 0);

        let item = Arc::new(vec![1, 2, 3]);
        cache.insert("key", item.clone()).await;
        assert_eq!(cache.size().await, 1);
        assert_eq!(cache.size_bytes().await, item_size);
        assert_eq!(cache.approx_size_bytes(), item_size);

        let retrieved = cache.get::<Vec<i32>>("key").await.unwrap();
        assert_eq!(*retrieved, *item);

        // Test eviction based on size
        for i in 0..20 {
            cache
                .insert(&format!("key_{}", i), Arc::new(vec![i, i, i]))
                .await;
        }
        assert_eq!(cache.size_bytes().await, capacity);
        assert_eq!(cache.size().await, 10);
    }

    #[tokio::test]
    async fn test_cache_trait_objects() {
        #[derive(Debug, DeepSizeOf)]
        struct MyType(i32);

        trait MyTrait: DeepSizeOf + Send + Sync + Any {
            fn as_any(&self) -> &dyn Any;
        }

        impl MyTrait for MyType {
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        let item = Arc::new(MyType(42));
        let item_dyn: Arc<dyn MyTrait> = item;

        let cache = LanceCache::with_capacity(1000);
        cache.insert_unsized("test", item_dyn).await;

        let retrieved = cache.get_unsized::<dyn MyTrait>("test").await.unwrap();
        let retrieved = retrieved.as_any().downcast_ref::<MyType>().unwrap();
        assert_eq!(retrieved.0, 42);
    }

    #[tokio::test]
    async fn test_cache_stats_basic() {
        let cache = LanceCache::with_capacity(1000);

        // Initially no hits or misses
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);

        // Miss on first get
        let result = cache.get::<Vec<i32>>("nonexistent");
        assert!(result.await.is_none());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        // Insert and then hit
        cache.insert("key1", Arc::new(vec![1, 2, 3])).await;
        let result = cache.get::<Vec<i32>>("key1");
        assert!(result.await.is_some());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        // Another hit
        let result = cache.get::<Vec<i32>>("key1");
        assert!(result.await.is_some());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);

        // Another miss
        let result = cache.get::<Vec<i32>>("nonexistent2");
        assert!(result.await.is_none());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 2);
    }

    #[tokio::test]
    async fn test_cache_stats_with_prefixes() {
        let base_cache = LanceCache::with_capacity(1000);
        let prefixed_cache = base_cache.with_key_prefix("test");

        // Stats should be shared between base and prefixed cache
        let stats = base_cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);

        let stats = prefixed_cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);

        // Miss on prefixed cache
        let result = prefixed_cache.get::<Vec<i32>>("key1");
        assert!(result.await.is_none());

        // Both should show the miss
        let stats = base_cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        let stats = prefixed_cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        // Insert through prefixed cache and hit
        prefixed_cache.insert("key1", Arc::new(vec![1, 2, 3])).await;
        let result = prefixed_cache.get::<Vec<i32>>("key1");
        assert!(result.await.is_some());

        // Both should show the hit
        let stats = base_cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        let stats = prefixed_cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_cache_stats_unsized() {
        #[derive(Debug, DeepSizeOf)]
        struct MyType(i32);

        trait MyTrait: DeepSizeOf + Send + Sync + Any {}

        impl MyTrait for MyType {}

        let cache = LanceCache::with_capacity(1000);

        // Miss on unsized get
        let result = cache.get_unsized::<dyn MyTrait>("test");
        assert!(result.await.is_none());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        // Insert and hit on unsized
        let item = Arc::new(MyType(42));
        let item_dyn: Arc<dyn MyTrait> = item;
        cache.insert_unsized("test", item_dyn).await;

        let result = cache.get_unsized::<dyn MyTrait>("test");
        assert!(result.await.is_some());
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_cache_stats_get_or_insert() {
        let cache = LanceCache::with_capacity(1000);

        // First call should be a miss and load the value
        let result: Arc<Vec<i32>> = cache
            .get_or_insert("key1".to_string(), |_key| async { Ok(vec![1, 2, 3]) })
            .await
            .unwrap();
        assert_eq!(*result, vec![1, 2, 3]);

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        // Second call should be a hit
        let result: Arc<Vec<i32>> = cache
            .get_or_insert("key1".to_string(), |_key| async {
                panic!("Should not be called")
            })
            .await
            .unwrap();
        assert_eq!(*result, vec![1, 2, 3]);

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        // Different key should be another miss
        let result: Arc<Vec<i32>> = cache
            .get_or_insert("key2".to_string(), |_key| async { Ok(vec![4, 5, 6]) })
            .await
            .unwrap();
        assert_eq!(*result, vec![4, 5, 6]);

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 2);
    }

    #[test]
    fn test_sized_record_type_name() {
        let record = SizedRecord::new(Arc::new(vec![1u8, 2, 3]));
        assert!(record.type_name.contains("Vec"));
        assert!(record.type_name.contains("u8"));
    }
}

#[cfg(all(test, feature = "debug-cache"))]
mod debug_tests {
    use super::*;

    #[tokio::test]
    async fn test_debug_sizes_basic() {
        let cache = LanceCache::with_capacity(10_000_000);

        // Insert some test data
        cache.insert("key1", Arc::new(vec![0u8; 1024])).await;
        cache.insert("key2", Arc::new(vec![0u8; 2048])).await;
        cache.insert("key3", Arc::new(vec![0u8; 4096])).await;

        let report = cache.debug_sizes(Some(42)).await;

        assert_eq!(report.entries.len(), 3);
        // At least the vec data should be counted
        assert!(report.total_deep_size_of > 7000);

        // After debug_sizes, cache should be empty
        assert_eq!(cache.size().await, 0);
    }

    #[tokio::test]
    async fn test_debug_sizes_empty_cache() {
        let cache = LanceCache::with_capacity(1000);
        let report = cache.debug_sizes(None).await;

        assert_eq!(report.entries.len(), 0);
        assert_eq!(report.total_deep_size_of, 0);
        assert_eq!(report.total_measured, 0);
        assert_eq!(report.entries_with_measurement, 0);
    }

    #[tokio::test]
    async fn test_debug_sizes_contains_all_entries() {
        let cache = LanceCache::with_capacity(10_000_000);
        cache.insert("a", Arc::new(vec![0u8; 100])).await;
        cache.insert("b", Arc::new(vec![0u8; 200])).await;
        cache.insert("c", Arc::new(vec![0u8; 300])).await;

        let report = cache.debug_sizes(Some(12345)).await;

        // All entries should be present (order may vary due to shuffling)
        let mut keys: Vec<_> = report.entries.iter().map(|e| e.key.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);

        // Verify type names are captured
        for entry in &report.entries {
            assert!(entry.type_name.contains("Vec"));
        }
    }

    #[test]
    fn test_cache_debug_report_discrepancy() {
        let report = CacheDebugReport {
            entries: vec![],
            total_deep_size_of: 1000,
            total_measured: 1200,
            entries_with_measurement: 1,
        };

        assert_eq!(report.discrepancy(), 200);
        assert!((report.discrepancy_percent() - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_cache_debug_report_format_table() {
        let report = CacheDebugReport {
            entries: vec![
                CacheDebugEntry {
                    key: "test/key1".to_string(),
                    type_name: "alloc::vec::Vec<u8>",
                    deep_size_of_bytes: 1024,
                    measured_bytes: Some(1000),
                },
                CacheDebugEntry {
                    key: "test/key2".to_string(),
                    type_name: "alloc::string::String",
                    deep_size_of_bytes: 256,
                    measured_bytes: None,
                },
            ],
            total_deep_size_of: 1280,
            total_measured: 1000,
            entries_with_measurement: 1,
        };

        let table = report.format_table();
        assert!(table.contains("Cache Debug Report"));
        assert!(table.contains("test/key1"));
        assert!(table.contains("test/key2"));
        assert!(table.contains("1024"));
        assert!(table.contains("1000"));
        assert!(table.contains("N/A"));
        assert!(table.contains("Total DeepSizeOf: 1280 bytes"));
    }
}
