// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::borrow::Cow;

/// Cache keys are structured as `user_key\0type_name`.
///
/// This function splits an opaque cache key into the user-visible portion
/// and the type_name string. Backend implementations can use this to inspect keys.
/// Returns `(empty slice, "")` if no separator is found.
pub fn parse_cache_key(key: &[u8]) -> (&[u8], &str) {
    if let Some(sep) = key.iter().position(|&b| b == 0) {
        let user_key = &key[..sep];
        let type_name = std::str::from_utf8(&key[sep + 1..]).unwrap_or("");
        (user_key, type_name)
    } else {
        (key, "")
    }
}

/// Build a key: `prefix/user_key\0type_name`.
pub(super) fn make_cache_key(prefix: &str, key: &str, type_name: &str) -> Vec<u8> {
    let user_key_len = if prefix.is_empty() {
        key.len()
    } else {
        prefix.len() + 1 + key.len()
    };
    let mut bytes = Vec::with_capacity(user_key_len + 1 + type_name.len());
    if !prefix.is_empty() {
        bytes.extend_from_slice(prefix.as_bytes());
        bytes.push(b'/');
    }
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(type_name.as_bytes());
    bytes
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
