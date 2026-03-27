// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{borrow::Cow, sync::Arc};

/// Structured cache key used by [`CacheBackend`](super::CacheBackend).
///
/// Composed of a prefix (scoping the key to a dataset/index), a user key
/// (identifying the specific entry), and a type name (distinguishing value
/// types that share the same user key).
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InternalCacheKey {
    prefix: Arc<str>,
    key: Arc<str>,
    type_name: &'static str,
}

impl InternalCacheKey {
    pub fn new(prefix: Arc<str>, key: Arc<str>, type_name: &'static str) -> Self {
        Self {
            prefix,
            key,
            type_name,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// Returns true if this key's prefix starts with the given string.
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.prefix.starts_with(prefix)
    }
}

pub trait CacheKey {
    type ValueType: 'static;

    fn key(&self) -> Cow<'_, str>;

    /// Short, stable string that distinguishes this value type from others in
    /// the cache. Used as the suffix in the encoded cache key (`user_key\0type_name`).
    ///
    /// **Must be unique per value type.** If two `CacheKey` impls return the
    /// same `type_name` but different `ValueType`s, entries will collide and
    /// downcasts will fail silently (returning `None` on get).
    ///
    /// Must be consistent across crate boundaries — use a short literal, not
    /// `std::any::type_name` pointers.
    fn type_name(&self) -> &'static str;
}

pub trait UnsizedCacheKey {
    type ValueType: 'static + ?Sized;

    fn key(&self) -> Cow<'_, str>;

    /// Short, stable string that distinguishes this value type from others in
    /// the cache. Must be unique per value type — collisions cause silent
    /// downcast failures.
    fn type_name(&self) -> &'static str;
}
