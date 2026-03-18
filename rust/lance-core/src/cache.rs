// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cache implementation
//!
//! This module provides a two-layer caching system:
//!
//! - [`CacheBackend`] is the low-level, pluggable trait that custom cache implementations
//!   can implement. It uses opaque byte keys and type-erased entries.
//! - [`LanceCache`] is the typed wrapper that handles key construction (prefix + type tag
//!   encoding), type-safe get/insert, and DeepSizeOf-based size computation.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use futures::{Future, FutureExt};
use tokio::sync::Mutex;

use crate::Result;

pub use deepsize::{Context, DeepSizeOf};

/// Result type used in the in-flight dedup map. Wraps errors in Arc so the
/// result can be cloned to multiple waiters.
type InFlightResult = std::result::Result<CacheEntry, Arc<crate::Error>>;
type InFlightMap = Mutex<HashMap<Vec<u8>, tokio::sync::watch::Receiver<Option<InFlightResult>>>>;

/// A type-erased cache entry.
pub type CacheEntry = Arc<dyn Any + Send + Sync>;

// ---------------------------------------------------------------------------
// CacheBackend trait
// ---------------------------------------------------------------------------

/// Low-level pluggable cache backend.
///
/// Implementations store entries keyed by opaque byte slices.
/// The [`LanceCache`] wrapper handles key construction and type safety;
/// backend authors do not need to worry about key encoding.
#[async_trait]
pub trait CacheBackend: Send + Sync + std::fmt::Debug {
    /// Look up an entry by its opaque key.
    async fn get(&self, key: &[u8]) -> Option<CacheEntry>;

    /// Store an entry. `size_bytes` is used for eviction accounting.
    async fn insert(&self, key: &[u8], entry: CacheEntry, size_bytes: usize);

    /// Remove all entries whose key starts with `prefix`.
    async fn invalidate_prefix(&self, prefix: &[u8]);

    /// Remove all entries.
    async fn clear(&self);

    /// Number of entries currently stored (may flush pending operations).
    async fn num_entries(&self) -> usize;

    /// Total weighted size in bytes of all stored entries (may flush pending operations).
    async fn size_bytes(&self) -> usize;

    /// Approximate number of entries, callable from synchronous contexts.
    /// Backends that cannot provide this cheaply should return 0.
    fn approx_num_entries(&self) -> usize {
        0
    }

    /// Approximate weighted size in bytes, callable from synchronous contexts.
    /// Backends that cannot provide this cheaply should return 0.
    fn approx_size_bytes(&self) -> usize {
        0
    }
}

// ---------------------------------------------------------------------------
// MokaCacheBackend — default moka-based implementation
// ---------------------------------------------------------------------------

/// Internal record stored in the moka cache.
#[derive(Clone, Debug)]
struct MokaCacheEntry {
    entry: CacheEntry,
    size_bytes: usize,
}

/// Default [`CacheBackend`] backed by a [moka](https://crates.io/crates/moka) cache.
///
/// Provides weighted-capacity eviction and concurrent-load deduplication.
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
        self.cache.weighted_size() as usize
    }
}

// ---------------------------------------------------------------------------
// LanceCache — typed wrapper around dyn CacheBackend
// ---------------------------------------------------------------------------

/// Typed cache wrapper that handles key construction and type safety.
///
/// Internally delegates to a [`CacheBackend`]. The default backend is
/// [`MokaCacheBackend`]; pass a custom backend via [`LanceCache::with_backend`].
#[derive(Clone)]
pub struct LanceCache {
    cache: Arc<dyn CacheBackend>,
    prefix: String,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    /// Deduplicates concurrent `get_or_insert` calls for the same key.
    in_flight: Arc<InFlightMap>,
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
        // This is a best-effort estimate; we can't iterate a dyn CacheBackend.
        // Callers should use stats().size_bytes for accurate numbers.
        0
    }
}

/// Returns a stable 8-byte discriminator for type `T`.
///
/// Uses the pointer of `std::any::type_name::<T>()`, which is a `&'static str`
/// with a process-lifetime-stable address. This is unique per monomorphized type
/// and avoids `transmute` on `TypeId`.
fn type_tag<T: 'static>() -> [u8; 8] {
    (std::any::type_name::<T>().as_ptr() as u64).to_le_bytes()
}

impl LanceCache {
    /// Build a key: `prefix/user_key\0<8-byte type tag>`.
    fn make_key<T: 'static>(&self, key: &str) -> Vec<u8> {
        let full_key = if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        };
        let mut bytes = full_key.into_bytes();
        bytes.push(0);
        bytes.extend_from_slice(&type_tag::<T>());
        bytes
    }

    /// Build a prefix (without type tag) for invalidation.
    fn make_prefix(&self, prefix: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, prefix).into_bytes()
    }
}

impl LanceCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: Arc::new(MokaCacheBackend::with_capacity(capacity)),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a cache backed by a custom [`CacheBackend`].
    pub fn with_backend(backend: Arc<dyn CacheBackend>) -> Self {
        Self {
            cache: backend,
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn no_cache() -> Self {
        Self {
            cache: Arc::new(MokaCacheBackend::no_cache()),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
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
            in_flight: self.in_flight.clone(),
        }
    }

    /// Invalidate all entries in the cache that start with the given prefix
    ///
    /// The given prefix is appended to the existing prefix of the cache. If you
    /// want to invalidate all at the current prefix, pass an empty string.
    pub fn invalidate_prefix(&self, prefix: &str) {
        let prefix_bytes = self.make_prefix(prefix);
        let cache = self.cache.clone();
        // Fire-and-forget; moka's invalidate_entries_if is synchronous under the hood
        // but our trait is async, so we spawn.
        tokio::spawn(async move {
            cache.invalidate_prefix(&prefix_bytes).await;
        });
    }

    pub async fn size(&self) -> usize {
        self.cache.num_entries().await
    }

    pub fn approx_size(&self) -> usize {
        self.cache.approx_num_entries()
    }

    pub async fn size_bytes(&self) -> usize {
        self.cache.size_bytes().await
    }

    pub fn approx_size_bytes(&self) -> usize {
        self.cache.approx_size_bytes()
    }

    async fn insert<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str, metadata: Arc<T>) {
        let size = metadata.deep_size_of() + 8; // +8 for the Arc pointer
        let cache_key = self.make_key::<T>(key);
        tracing::trace!(
            target: "lance_cache::insert",
            key = key,
            type_id = std::any::type_name::<T>(),
            size = size,
        );
        self.cache.insert(&cache_key, metadata, size).await;
    }

    pub async fn insert_unsized<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
        metadata: Arc<T>,
    ) {
        // Wrap in another Arc to make the data Sized.
        self.insert(key, Arc::new(metadata)).await
    }

    async fn get<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let cache_key = self.make_key::<T>(key);
        if let Some(entry) = self.cache.get(&cache_key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.downcast::<T>().unwrap())
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

    /// Get an item, or load it if not cached.
    ///
    /// Concurrent calls for the same key are deduplicated: only the first
    /// caller runs the loader; subsequent callers wait for the result.
    async fn get_or_insert<T: DeepSizeOf + Send + Sync + 'static, F, Fut>(
        &self,
        key: String,
        loader: F,
    ) -> Result<Arc<T>>
    where
        F: FnOnce(&str) -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        let cache_key = self.make_key::<T>(&key);

        // Fast path: already cached.
        if let Some(entry) = self.cache.get(&cache_key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(entry.downcast::<T>().unwrap());
        }

        // Check for an in-flight load for this key.
        {
            let map = self.in_flight.lock().await;
            if let Some(rx) = map.get(&cache_key) {
                let mut rx = rx.clone();
                drop(map);
                // Wait until the leader finishes.
                let result = rx
                    .wait_for(|v| v.is_some())
                    .await
                    .map_err(|_| crate::Error::internal("In-flight cache loader was dropped"))?
                    .as_ref()
                    .unwrap()
                    .clone();
                match result {
                    Ok(entry) => {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(entry.downcast::<T>().unwrap());
                    }
                    Err(err) => {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                        return Err(crate::Error::internal(format!(
                            "Cache loader failed: {err}"
                        )));
                    }
                }
            }
        }

        // We are the leader. Register our in-flight entry.
        let (tx, rx) = tokio::sync::watch::channel(None);
        {
            let mut map = self.in_flight.lock().await;
            map.insert(cache_key.clone(), rx);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let result = loader(&key).await;

        // Clean up the in-flight entry before sending, so new arrivals
        // go through the normal cache path.
        {
            let mut map = self.in_flight.lock().await;
            map.remove(&cache_key);
        }

        match result {
            Ok(value) => {
                let arc = Arc::new(value);
                let size = arc.deep_size_of() + 8;
                self.cache.insert(&cache_key, arc.clone(), size).await;
                let _ = tx.send(Some(Ok(arc.clone() as CacheEntry)));
                Ok(arc)
            }
            Err(err) => {
                let shared_err = Arc::new(err);
                let _ = tx.send(Some(Err(shared_err.clone())));
                Err(crate::Error::internal(format!(
                    "Cache loader failed: {shared_err}"
                )))
            }
        }
    }

    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            num_entries: self.cache.num_entries().await,
            size_bytes: self.cache.size_bytes().await,
        }
    }

    pub async fn clear(&self) {
        self.cache.clear().await;
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
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

// ---------------------------------------------------------------------------
// WeakLanceCache
// ---------------------------------------------------------------------------

/// A weak reference to a LanceCache, used by indices to avoid circular references.
/// When the original cache is dropped, operations on this will gracefully no-op.
#[derive(Clone, Debug)]
pub struct WeakLanceCache {
    inner: std::sync::Weak<dyn CacheBackend>,
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

    /// Build a key: `prefix/user_key\0<8-byte type tag>`.
    fn make_key<T: 'static>(&self, key: &str) -> Vec<u8> {
        let full_key = if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        };
        let mut bytes = full_key.into_bytes();
        bytes.push(0);
        bytes.extend_from_slice(&type_tag::<T>());
        bytes
    }

    /// Get an item from cache if the cache is still alive
    pub async fn get<T: DeepSizeOf + Send + Sync + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let cache = self.inner.upgrade()?;
        let cache_key = self.make_key::<T>(key);
        if let Some(entry) = cache.get(&cache_key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.downcast::<T>().unwrap())
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
            let size = value.deep_size_of() + 8;
            let cache_key = self.make_key::<T>(key);
            cache.insert(&cache_key, value, size).await;
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
            let cache_key = self.make_key::<T>(key);

            if let Some(entry) = cache.get(&cache_key).await {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.downcast::<T>().unwrap());
            }

            self.misses.fetch_add(1, Ordering::Relaxed);
            let value = f().await?;
            let arc = Arc::new(value);
            let size = arc.deep_size_of() + 8;
            cache.insert(&cache_key, arc.clone(), size).await;
            Ok(arc)
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
        let cache = self.inner.upgrade()?;
        let cache_key = self.make_key::<Arc<T>>(key);
        if let Some(entry) = cache.get(&cache_key).await {
            entry
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
            let wrapper = Arc::new(value);
            let size = wrapper.deep_size_of() + 8;
            let cache_key = self.make_key::<Arc<T>>(key);
            cache.insert(&cache_key, wrapper, size).await;
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

// ---------------------------------------------------------------------------
// CacheKey traits
// ---------------------------------------------------------------------------

pub trait CacheKey {
    type ValueType;

    fn key(&self) -> Cow<'_, str>;
}

pub trait UnsizedCacheKey {
    type ValueType: ?Sized;

    fn key(&self) -> Cow<'_, str>;
}

// ---------------------------------------------------------------------------
// CacheStats
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_bytes() {
        let item = Arc::new(vec![1, 2, 3]);
        let item_size = item.deep_size_of(); // Size of Arc<Vec<i32>>
        let capacity = 10 * item_size;

        let cache = LanceCache::with_capacity(capacity);

        let item = Arc::new(vec![1, 2, 3]);
        cache.insert("key", item.clone()).await;
        assert_eq!(cache.size().await, 1);

        let retrieved = cache.get::<Vec<i32>>("key").await.unwrap();
        assert_eq!(*retrieved, *item);

        // Test eviction based on size
        for i in 0..20 {
            cache
                .insert(&format!("key_{}", i), Arc::new(vec![i, i, i]))
                .await;
        }
        // Moka evicts based on weighted size; after run_pending_tasks, the size
        // should be bounded by capacity.
        assert!(cache.size_bytes().await <= capacity);
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

    #[tokio::test]
    async fn test_custom_backend() {
        use std::collections::HashMap;
        use tokio::sync::Mutex;

        /// A simple HashMap-based cache backend for testing.
        #[derive(Debug)]
        struct HashMapBackend {
            map: Mutex<HashMap<Vec<u8>, (CacheEntry, usize)>>,
        }

        impl HashMapBackend {
            fn new() -> Self {
                Self {
                    map: Mutex::new(HashMap::new()),
                }
            }
        }

        #[async_trait]
        impl CacheBackend for HashMapBackend {
            async fn get(&self, key: &[u8]) -> Option<CacheEntry> {
                self.map.lock().await.get(key).map(|(e, _)| e.clone())
            }

            async fn insert(&self, key: &[u8], entry: CacheEntry, size_bytes: usize) {
                self.map
                    .lock()
                    .await
                    .insert(key.to_vec(), (entry, size_bytes));
            }

            async fn invalidate_prefix(&self, prefix: &[u8]) {
                self.map.lock().await.retain(|k, _| !k.starts_with(prefix));
            }

            async fn clear(&self) {
                self.map.lock().await.clear();
            }

            async fn num_entries(&self) -> usize {
                self.map.lock().await.len()
            }

            async fn size_bytes(&self) -> usize {
                self.map.lock().await.values().map(|(_, s)| *s).sum()
            }
        }

        let backend = Arc::new(HashMapBackend::new());
        let cache = LanceCache::with_backend(backend);

        // Insert and retrieve
        cache.insert("key1", Arc::new(vec![1, 2, 3])).await;
        let retrieved = cache.get::<Vec<i32>>("key1").await.unwrap();
        assert_eq!(*retrieved, vec![1, 2, 3]);

        // Miss for different type at same key
        let miss = cache.get::<Vec<u8>>("key1").await;
        assert!(miss.is_none());

        // Stats tracking works
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.num_entries, 1);
    }

    #[tokio::test]
    async fn test_get_or_insert_dedup() {
        use std::sync::atomic::AtomicUsize;

        let load_count = Arc::new(AtomicUsize::new(0));
        let cache = LanceCache::with_capacity(10000);

        // Launch several concurrent get_or_insert calls for the same key.
        let (barrier_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let mut handles = Vec::new();
        for _ in 0..5 {
            let cache = cache.clone();
            let load_count = load_count.clone();
            let mut barrier_rx = barrier_tx.subscribe();
            handles.push(tokio::spawn(async move {
                barrier_rx.recv().await.ok();
                cache
                    .get_or_insert("key".to_string(), |_key| {
                        let load_count = load_count.clone();
                        async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            // Simulate slow load so other tasks can pile up.
                            tokio::task::yield_now().await;
                            Ok(vec![1, 2, 3])
                        }
                    })
                    .await
            }));
        }
        // Release all tasks at once.
        barrier_tx.send(()).unwrap();
        for h in handles {
            let result: Arc<Vec<i32>> = h.await.unwrap().unwrap();
            assert_eq!(*result, vec![1, 2, 3]);
        }

        // The loader should have run exactly once.
        assert_eq!(load_count.load(Ordering::SeqCst), 1);
    }
}
