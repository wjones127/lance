// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hash::Hasher;

// A wrapper for &[u8] to allow &[u8] as hash keys,
// the equality for this `U8SliceKey` means that the &[u8] contents are equal.
#[derive(Debug, Eq)]
pub struct U8SliceKey<'a>(pub &'a [u8]);
impl PartialEq for U8SliceKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::hash::Hash for U8SliceKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_u8_slice_key_eq() {
        let a = U8SliceKey(&[1, 2, 3]);
        let b = U8SliceKey(&[1, 2, 3]);
        let c = U8SliceKey(&[1, 2, 4]);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_u8_slice_key_hash() {
        let mut map: HashMap<U8SliceKey, i32> = HashMap::new();
        map.insert(U8SliceKey(&[1, 2, 3]), 42);
        assert_eq!(map.get(&U8SliceKey(&[1, 2, 3])), Some(&42));
        assert_eq!(map.get(&U8SliceKey(&[1, 2, 4])), None);
    }
}
