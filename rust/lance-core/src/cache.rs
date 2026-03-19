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
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use futures::{Future, FutureExt};

use crate::Result;

pub use deepsize::{Context, DeepSizeOf};

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

    /// Get an existing entry or compute it from `loader`.
    ///
    /// Implementations should deduplicate concurrent loads for the same key
    /// so the loader runs at most once.
    ///
    /// The loader is a pinned future that produces `(entry, size_bytes)`.
    /// It borrows from the caller's scope and will be `.await`ed within
    /// this method — implementations must not store it beyond the call.
    async fn get_or_insert<'a>(
        &self,
        key: &[u8],
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
    ) -> Result<CacheEntry>;

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
    /// Used by `DeepSizeOf` to report cache memory usage.
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

// ---------------------------------------------------------------------------
// Type identity helpers
// ---------------------------------------------------------------------------

/// Cache keys are structured as `user_key\0type_id`.
///
/// This function splits an opaque cache key into the user-visible portion
/// and the type_id string. Backend implementations can use this to inspect keys.
/// Returns `(empty slice, "")` if no separator is found.
pub fn parse_cache_key(key: &[u8]) -> (&[u8], &str) {
    if let Some(sep) = key.iter().position(|&b| b == 0) {
        let user_key = &key[..sep];
        let type_id = std::str::from_utf8(&key[sep + 1..]).unwrap_or("");
        (user_key, type_id)
    } else {
        (key, "")
    }
}

/// Build a key: `prefix/user_key\0type_id`.
fn make_cache_key(prefix: &str, key: &str, type_id: &str) -> Vec<u8> {
    let full_key = if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{}/{}", prefix, key)
    };
    let mut bytes = full_key.into_bytes();
    bytes.push(0);
    bytes.extend_from_slice(type_id.as_bytes());
    bytes
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
        self.cache.approx_size_bytes()
    }
}

impl LanceCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: Arc::new(MokaCacheBackend::with_capacity(capacity)),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a cache backed by a custom [`CacheBackend`].
    pub fn with_backend(backend: Arc<dyn CacheBackend>) -> Self {
        Self {
            cache: backend,
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn no_cache() -> Self {
        Self {
            cache: Arc::new(MokaCacheBackend::no_cache()),
            prefix: String::new(),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Appends a prefix to the cache key.
    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            cache: self.cache.clone(),
            prefix: format!("{}{}/", self.prefix, prefix),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }

    /// Invalidate all entries whose key starts with the given prefix.
    pub async fn invalidate_prefix(&self, prefix: &str) {
        let prefix_bytes = format!("{}{}", self.prefix, prefix).into_bytes();
        self.cache.invalidate_prefix(&prefix_bytes).await;
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

    // -- Sized insert/get (internal, used by CacheKey methods) ----------------

    async fn insert_with_id<T: DeepSizeOf + Send + Sync + 'static>(
        &self,
        key: &str,
        type_id: &str,
        metadata: Arc<T>,
    ) {
        let size = metadata.deep_size_of() + 8;
        let cache_key = make_cache_key(&self.prefix, key, type_id);
        self.cache.insert(&cache_key, metadata, size).await;
    }

    async fn get_with_id<T: Send + Sync + 'static>(
        &self,
        key: &str,
        type_id: &str,
    ) -> Option<Arc<T>> {
        let cache_key = make_cache_key(&self.prefix, key, type_id);
        if let Some(entry) = self.cache.get(&cache_key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.downcast::<T>().unwrap())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    async fn get_or_insert_with_id<T: DeepSizeOf + Send + Sync + 'static, F, Fut>(
        &self,
        key: &str,
        type_id: &str,
        loader: F,
    ) -> Result<Arc<T>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        let cache_key = make_cache_key(&self.prefix, key, type_id);

        // Type-erase the loader into a pinned future for the backend.
        let typed_loader = Box::pin(async move {
            let value = loader().await?;
            let arc = Arc::new(value);
            let size = arc.deep_size_of() + 8;
            Ok((arc as CacheEntry, size))
        });

        let entry = self.cache.get_or_insert(&cache_key, typed_loader).await?;

        // Track hit/miss based on whether we got a pre-existing entry.
        // (Approximate: we can't distinguish "backend had it" from "loader ran"
        // without a richer return type. Count all get_or_insert as misses for now.)
        self.misses.fetch_add(1, Ordering::Relaxed);

        Ok(entry.downcast::<T>().unwrap())
    }

    // -- Unsized insert/get ---------------------------------------------------

    async fn insert_unsized_with_id<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
        type_id: &str,
        metadata: Arc<T>,
    ) {
        self.insert_with_id(key, type_id, Arc::new(metadata)).await
    }

    async fn get_unsized_with_id<T: DeepSizeOf + Send + Sync + 'static + ?Sized>(
        &self,
        key: &str,
        type_id: &str,
    ) -> Option<Arc<T>> {
        let outer = self.get_with_id::<Arc<T>>(key, type_id).await?;
        Some(outer.as_ref().clone())
    }

    // -- Stats / clear --------------------------------------------------------

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

    // -- CacheKey-based methods -----------------------------------------------

    pub async fn insert_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.insert_with_id(&cache_key.key(), cache_key.type_id(), metadata)
            .boxed()
            .await
    }

    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.get_with_id::<K::ValueType>(&cache_key.key(), cache_key.type_id())
            .boxed()
            .await
    }

    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        let type_id = cache_key.type_id();
        let key_str = cache_key.key().into_owned();
        Box::pin(self.get_or_insert_with_id(&key_str, type_id, loader)).await
    }

    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.insert_unsized_with_id(&cache_key.key(), cache_key.type_id(), metadata)
            .boxed()
            .await
    }

    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.get_unsized_with_id::<K::ValueType>(&cache_key.key(), cache_key.type_id())
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
    pub fn from(cache: &LanceCache) -> Self {
        Self {
            inner: Arc::downgrade(&cache.cache),
            prefix: cache.prefix.clone(),
            hits: cache.hits.clone(),
            misses: cache.misses.clone(),
        }
    }

    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            inner: self.inner.clone(),
            prefix: format!("{}{}/", self.prefix, prefix),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }

    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let cache = self.inner.upgrade()?;
        let key = make_cache_key(&self.prefix, &cache_key.key(), cache_key.type_id());
        if let Some(entry) = cache.get(&key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.downcast::<K::ValueType>().unwrap())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub async fn insert_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>) -> bool
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        if let Some(cache) = self.inner.upgrade() {
            let size = value.deep_size_of() + 8;
            let key = make_cache_key(&self.prefix, &cache_key.key(), cache_key.type_id());
            cache.insert(&key, value, size).await;
            true
        } else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert item");
            false
        }
    }

    /// Get or insert an item, computing it if necessary.
    ///
    /// Deduplication of concurrent loads is handled by the backend.
    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        if let Some(cache) = self.inner.upgrade() {
            let key = make_cache_key(&self.prefix, &cache_key.key(), cache_key.type_id());
            let typed_loader = Box::pin(async move {
                let value = loader().await?;
                let arc = Arc::new(value);
                let size = arc.deep_size_of() + 8;
                Ok((arc as CacheEntry, size))
            });
            let entry = cache.get_or_insert(&key, typed_loader).await?;
            self.misses.fetch_add(1, Ordering::Relaxed);
            Ok(entry.downcast::<K::ValueType>().unwrap())
        } else {
            log::warn!("WeakLanceCache: cache no longer available, computing without caching");
            loader().await.map(Arc::new)
        }
    }

    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let cache = self.inner.upgrade()?;
        let key = make_cache_key(&self.prefix, &cache_key.key(), cache_key.type_id());
        if let Some(entry) = cache.get(&key).await {
            entry
                .downcast::<Arc<K::ValueType>>()
                .ok()
                .map(|arc| arc.as_ref().clone())
        } else {
            None
        }
    }

    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        if let Some(cache) = self.inner.upgrade() {
            let wrapper = Arc::new(value);
            let size = wrapper.deep_size_of() + 8;
            let key = make_cache_key(&self.prefix, &cache_key.key(), cache_key.type_id());
            cache.insert(&key, wrapper, size).await;
        } else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert unsized item");
        }
    }
}

// ---------------------------------------------------------------------------
// CacheKey traits
// ---------------------------------------------------------------------------

pub trait CacheKey {
    type ValueType: 'static;

    fn key(&self) -> Cow<'_, str>;

    /// Short, stable string that distinguishes this value type from others in
    /// the cache. Used as the suffix in the encoded cache key (`user_key\0type_id`).
    /// Must be consistent across crate boundaries — use a short literal, not
    /// `type_name` pointers.
    fn type_id(&self) -> &'static str;
}

pub trait UnsizedCacheKey {
    type ValueType: 'static + ?Sized;

    fn key(&self) -> Cow<'_, str>;

    fn type_id(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// CacheStats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub num_entries: usize,
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
    use std::collections::HashMap;
    use std::marker::PhantomData;

    struct TestKey<T: 'static> {
        key: String,
        _phantom: PhantomData<T>,
    }

    impl<T: 'static> TestKey<T> {
        fn new(key: &str) -> Self {
            Self {
                key: key.to_string(),
                _phantom: PhantomData,
            }
        }
    }

    impl<T: 'static> CacheKey for TestKey<T> {
        type ValueType = T;
        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.key)
        }
        fn type_id(&self) -> &'static str {
            std::any::type_name::<T>()
        }
    }

    /// Test helper: an UnsizedCacheKey for trait object values.
    struct TestUnsizedKey<T: 'static + ?Sized> {
        key: String,
        _phantom: PhantomData<T>,
    }

    impl<T: 'static + ?Sized> TestUnsizedKey<T> {
        fn new(key: &str) -> Self {
            Self {
                key: key.to_string(),
                _phantom: PhantomData,
            }
        }
    }

    impl<T: 'static + ?Sized> UnsizedCacheKey for TestUnsizedKey<T> {
        type ValueType = T;
        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.key)
        }
        fn type_id(&self) -> &'static str {
            std::any::type_name::<T>()
        }
    }

    #[tokio::test]
    async fn test_cache_bytes() {
        let item = Arc::new(vec![1, 2, 3]);
        let item_size = item.deep_size_of();
        let capacity = 10 * item_size;
        let cache = LanceCache::with_capacity(capacity);

        cache
            .insert_with_key(&TestKey::<Vec<i32>>::new("key"), item.clone())
            .await;
        assert_eq!(cache.size().await, 1);

        let retrieved = cache
            .get_with_key(&TestKey::<Vec<i32>>::new("key"))
            .await
            .unwrap();
        assert_eq!(*retrieved, *item);

        for i in 0..20 {
            cache
                .insert_with_key(
                    &TestKey::<Vec<i32>>::new(&format!("key_{}", i)),
                    Arc::new(vec![i, i, i]),
                )
                .await;
        }
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

        let item: Arc<dyn MyTrait> = Arc::new(MyType(42));
        let cache = LanceCache::with_capacity(1000);
        cache
            .insert_unsized_with_key(&TestUnsizedKey::<dyn MyTrait>::new("test"), item)
            .await;

        let retrieved = cache
            .get_unsized_with_key(&TestUnsizedKey::<dyn MyTrait>::new("test"))
            .await
            .unwrap();
        assert_eq!(retrieved.as_any().downcast_ref::<MyType>().unwrap().0, 42);
    }

    #[tokio::test]
    async fn test_cache_stats_basic() {
        let cache = LanceCache::with_capacity(1000);
        assert_eq!(cache.stats().await.hits, 0);

        // Miss
        assert!(
            cache
                .get_with_key(&TestKey::<Vec<i32>>::new("x"))
                .await
                .is_none()
        );
        assert_eq!(cache.stats().await.misses, 1);

        // Insert then hit
        cache
            .insert_with_key(&TestKey::new("k"), Arc::new(vec![1, 2, 3]))
            .await;
        assert!(
            cache
                .get_with_key(&TestKey::<Vec<i32>>::new("k"))
                .await
                .is_some()
        );
        assert_eq!(cache.stats().await.hits, 1);
    }

    #[tokio::test]
    async fn test_cache_stats_with_prefixes() {
        let base = LanceCache::with_capacity(1000);
        let prefixed = base.with_key_prefix("ns");

        assert!(
            prefixed
                .get_with_key(&TestKey::<Vec<i32>>::new("k"))
                .await
                .is_none()
        );
        assert_eq!(base.stats().await.misses, 1);

        prefixed
            .insert_with_key(&TestKey::new("k"), Arc::new(vec![1]))
            .await;
        assert!(
            prefixed
                .get_with_key(&TestKey::<Vec<i32>>::new("k"))
                .await
                .is_some()
        );
        assert_eq!(base.stats().await.hits, 1);
    }

    #[tokio::test]
    async fn test_cache_get_or_insert() {
        let cache = LanceCache::with_capacity(1000);

        let v: Arc<Vec<i32>> = cache
            .get_or_insert_with_key(TestKey::<Vec<i32>>::new("k"), || async {
                Ok(vec![1, 2, 3])
            })
            .await
            .unwrap();
        assert_eq!(*v, vec![1, 2, 3]);

        // Second call should not invoke loader
        let v: Arc<Vec<i32>> = cache
            .get_or_insert_with_key(TestKey::<Vec<i32>>::new("k"), || async {
                panic!("should not be called")
            })
            .await
            .unwrap();
        assert_eq!(*v, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_custom_backend() {
        use tokio::sync::Mutex;

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
            async fn get_or_insert<'a>(
                &self,
                key: &[u8],
                loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
            ) -> Result<CacheEntry> {
                if let Some((entry, _)) = self.map.lock().await.get(key) {
                    Ok(entry.clone())
                } else {
                    let (entry, size) = loader.await?;
                    self.map
                        .lock()
                        .await
                        .insert(key.to_vec(), (entry.clone(), size));
                    Ok(entry)
                }
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

        let cache = LanceCache::with_backend(Arc::new(HashMapBackend::new()));

        cache
            .insert_with_key(&TestKey::new("k"), Arc::new(vec![1, 2, 3]))
            .await;
        assert!(
            cache
                .get_with_key(&TestKey::<Vec<i32>>::new("k"))
                .await
                .is_some()
        );
        // Different type at same key = miss
        assert!(
            cache
                .get_with_key(&TestKey::<Vec<u8>>::new("k"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_get_or_insert_dedup() {
        use std::sync::atomic::AtomicUsize;

        let load_count = Arc::new(AtomicUsize::new(0));
        let cache = LanceCache::with_capacity(10000);

        let (barrier_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let mut handles = Vec::new();
        for _ in 0..5 {
            let cache = cache.clone();
            let load_count = load_count.clone();
            let mut barrier_rx = barrier_tx.subscribe();
            handles.push(tokio::spawn(async move {
                barrier_rx.recv().await.ok();
                cache
                    .get_or_insert_with_key(TestKey::<Vec<i32>>::new("key"), || {
                        let load_count = load_count.clone();
                        async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Ok(vec![1, 2, 3])
                        }
                    })
                    .await
            }));
        }
        barrier_tx.send(()).unwrap();
        for h in handles {
            let result: Arc<Vec<i32>> = h.await.unwrap().unwrap();
            assert_eq!(*result, vec![1, 2, 3]);
        }

        assert_eq!(load_count.load(Ordering::SeqCst), 1);
    }
}
