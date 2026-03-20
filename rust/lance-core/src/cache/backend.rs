// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;

use crate::Result;

/// A type-erased cache entry.
pub type CacheEntry = Arc<dyn Any + Send + Sync>;

/// Low-level pluggable cache backend.
///
/// Implementations store entries keyed by opaque byte slices.
/// The [`LanceCache`](super::LanceCache) wrapper handles key construction and type safety;
/// backend authors do not need to worry about key encoding.
///
/// Keys are structured as `user_key\0type_name` where `type_name` comes from
/// [`CacheKey::type_name()`](super::CacheKey::type_name). Backend authors who need to
/// inspect keys can use [`parse_cache_key()`](super::parse_cache_key) to split them.
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
    /// The loader is a pinned, boxed future rather than a generic closure
    /// because `async_trait` erases the `Self` lifetime, making it impossible
    /// to express a generic closure whose returned future borrows from the
    /// caller. Boxing the future once at the call site (in `LanceCache`)
    /// avoids this lifetime conflict while keeping the trait object-safe.
    ///
    /// The future borrows from the caller's scope and will be `.await`ed within
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
