// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Metadata for index

use std::sync::Arc;

use chrono::{DateTime, Utc};
use deepsize::DeepSizeOf;
use roaring::RoaringBitmap;
use snafu::location;
use uuid::Uuid;

use super::pb;
use lance_core::{Error, Result};

/// Index metadata
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    /// Unique ID across all dataset versions.
    pub uuid: Uuid,

    /// Fields to build the index.
    pub fields: Vec<i32>,

    /// Human readable index name
    pub name: String,

    /// The version of the dataset this index was last updated on
    ///
    /// This is set when the index is created (based on the version used to train the index)
    /// This is updated when the index is updated or remapped
    pub dataset_version: u64,

    /// The fragment ids this index covers.
    ///
    /// This may contain fragment ids that no longer exist in the dataset. It should **not**
    /// be modified after it's initial creation. To indicate that certain fragments are invalidated,
    /// use the `invalidated_fragments` field instead.
    ///
    /// If this is None, then this is unknown.
    pub fragment_bitmap: Option<RoaringBitmap>,

    /// The fragment ids that have been invalidated in this index.
    ///
    /// If a fragment is invalidated, it means that the index should not be used to query data from that fragment.
    /// This is typically used when the indexed column is updated in-place in a fragment (using a DataReplacement transaction, for example).
    /// If this is None, then assume no fragments are invalidated.
    pub invalidated_fragments: Option<RoaringBitmap>,

    /// Metadata specific to the index type
    ///
    /// This is an Option because older versions of Lance may not have this defined.  However, it should always
    /// be present in newer versions.
    pub index_details: Option<Arc<prost_types::Any>>,

    /// The index version.
    pub index_version: i32,

    /// Timestamp when the index was created
    ///
    /// This field is optional for backward compatibility. For existing indices created before
    /// this field was added, this will be None.
    pub created_at: Option<DateTime<Utc>>,

    /// The base path index of the index files. Used when the index is imported or referred from another dataset.
    /// Lance uses it as key of the base_paths field in Manifest to determine the actual base path of the index files.
    pub base_id: Option<u32>,
}

impl IndexMetadata {
    /// Returns the effective fragment bitmap for this index.
    ///
    /// This is the set of fragments that the index can be used to query. It is computed by:
    /// 1. Starting with the fragments the index was built on (`fragment_bitmap`)
    /// 2. Intersecting with fragments that still exist in the dataset (`existing_fragments`)
    /// 3. Subtracting any fragments that have been invalidated (`invalidated_fragments`)
    ///
    /// Returns `None` if the index has no `fragment_bitmap` set.
    pub fn effective_fragment_bitmap(
        &self,
        existing_fragments: &RoaringBitmap,
    ) -> Option<RoaringBitmap> {
        let fragment_bitmap = self.fragment_bitmap.as_ref()?;
        let mut result = fragment_bitmap & existing_fragments;
        if let Some(invalidated) = &self.invalidated_fragments {
            result -= invalidated;
        }
        Some(result)
    }
}

impl DeepSizeOf for IndexMetadata {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.uuid.as_bytes().deep_size_of_children(context)
            + self.fields.deep_size_of_children(context)
            + self.name.deep_size_of_children(context)
            + self.dataset_version.deep_size_of_children(context)
            + self
                .fragment_bitmap
                .as_ref()
                .map(|fragment_bitmap| fragment_bitmap.serialized_size())
                .unwrap_or(0)
    }
}

impl TryFrom<pb::IndexMetadata> for IndexMetadata {
    type Error = Error;

    fn try_from(proto: pb::IndexMetadata) -> Result<Self> {
        let fragment_bitmap = if proto.fragment_bitmap.is_empty() {
            None
        } else {
            Some(RoaringBitmap::deserialize_from(
                &mut proto.fragment_bitmap.as_slice(),
            )?)
        };

        let invalidated_fragments = if proto.invalidated_fragments.is_empty() {
            None
        } else {
            Some(RoaringBitmap::deserialize_from(
                &mut proto.invalidated_fragments.as_slice(),
            )?)
        };

        Ok(Self {
            uuid: proto.uuid.as_ref().map(Uuid::try_from).ok_or_else(|| {
                Error::io(
                    "uuid field does not exist in Index metadata".to_string(),
                    location!(),
                )
            })??,
            name: proto.name,
            fields: proto.fields,
            dataset_version: proto.dataset_version,
            fragment_bitmap,
            invalidated_fragments,
            index_details: proto.index_details.map(Arc::new),
            index_version: proto.index_version.unwrap_or_default(),
            created_at: proto.created_at.map(|ts| {
                DateTime::from_timestamp_millis(ts as i64)
                    .expect("Invalid timestamp in index metadata")
            }),
            base_id: proto.base_id,
        })
    }
}

impl From<&IndexMetadata> for pb::IndexMetadata {
    fn from(idx: &IndexMetadata) -> Self {
        let mut fragment_bitmap = Vec::new();
        if let Some(bitmap) = &idx.fragment_bitmap {
            fragment_bitmap.reserve(bitmap.serialized_size());
            if let Err(e) = bitmap.serialize_into(&mut fragment_bitmap) {
                // In theory, this should never error. But if we do, just
                // recover gracefully.
                log::error!("Failed to serialize fragment bitmap: {}", e);
                fragment_bitmap.clear();
            }
        }

        let mut invalidated_fragments = Vec::new();
        if let Some(bitmap) = &idx.invalidated_fragments {
            invalidated_fragments.reserve(bitmap.serialized_size());
            if let Err(e) = bitmap.serialize_into(&mut invalidated_fragments) {
                // In theory, this should never error. But if we do, just
                // recover gracefully.
                log::error!("Failed to serialize invalidated fragments bitmap: {}", e);
                invalidated_fragments.clear();
            }
        }

        Self {
            uuid: Some((&idx.uuid).into()),
            name: idx.name.clone(),
            fields: idx.fields.clone(),
            dataset_version: idx.dataset_version,
            fragment_bitmap,
            invalidated_fragments,
            index_details: idx
                .index_details
                .as_ref()
                .map(|details| details.as_ref().clone()),
            index_version: Some(idx.index_version),
            created_at: idx.created_at.map(|dt| dt.timestamp_millis() as u64),
            base_id: idx.base_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_index(
        fragment_bitmap: Option<RoaringBitmap>,
        invalidated_fragments: Option<RoaringBitmap>,
    ) -> IndexMetadata {
        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![0],
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap,
            invalidated_fragments,
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
        }
    }

    #[test]
    fn test_effective_fragment_bitmap_excludes_invalidated() {
        // Index covers fragments 0, 1, 2, 3
        let fragment_bitmap = RoaringBitmap::from_iter([0, 1, 2, 3]);
        // Fragment 1 and 2 have been invalidated (indexed column was updated in place)
        let invalidated_fragments = RoaringBitmap::from_iter([1, 2]);
        let index = make_test_index(Some(fragment_bitmap), Some(invalidated_fragments));

        // Dataset currently has fragments 0, 1, 2, 3, 4
        let existing_fragments = RoaringBitmap::from_iter([0, 1, 2, 3, 4]);

        let effective = index
            .effective_fragment_bitmap(&existing_fragments)
            .unwrap();

        // Should only include 0 and 3 (not 1, 2 which are invalidated, not 4 which wasn't indexed)
        assert_eq!(effective, RoaringBitmap::from_iter([0, 3]));
    }

    #[test]
    fn test_effective_fragment_bitmap_no_invalidated() {
        // Index covers fragments 0, 1, 2
        let fragment_bitmap = RoaringBitmap::from_iter([0, 1, 2]);
        let index = make_test_index(Some(fragment_bitmap), None);

        // Dataset currently has fragments 0, 1, 2, 3
        let existing_fragments = RoaringBitmap::from_iter([0, 1, 2, 3]);

        let effective = index
            .effective_fragment_bitmap(&existing_fragments)
            .unwrap();

        // Should include all indexed fragments that still exist
        assert_eq!(effective, RoaringBitmap::from_iter([0, 1, 2]));
    }

    #[test]
    fn test_effective_fragment_bitmap_empty_invalidated() {
        // Index covers fragments 0, 1, 2
        let fragment_bitmap = RoaringBitmap::from_iter([0, 1, 2]);
        let index = make_test_index(Some(fragment_bitmap), Some(RoaringBitmap::new()));

        let existing_fragments = RoaringBitmap::from_iter([0, 1, 2, 3]);

        let effective = index
            .effective_fragment_bitmap(&existing_fragments)
            .unwrap();

        // Empty invalidated should not affect the result
        assert_eq!(effective, RoaringBitmap::from_iter([0, 1, 2]));
    }

    #[test]
    fn test_effective_fragment_bitmap_deleted_and_invalidated() {
        // Index covers fragments 0, 1, 2, 3
        let fragment_bitmap = RoaringBitmap::from_iter([0, 1, 2, 3]);
        // Fragment 2 was invalidated
        let invalidated_fragments = RoaringBitmap::from_iter([2]);
        let index = make_test_index(Some(fragment_bitmap), Some(invalidated_fragments));

        // Dataset currently has fragments 0, 2, 3 (fragment 1 was deleted)
        let existing_fragments = RoaringBitmap::from_iter([0, 2, 3]);

        let effective = index
            .effective_fragment_bitmap(&existing_fragments)
            .unwrap();

        // Should exclude both deleted (1) and invalidated (2)
        assert_eq!(effective, RoaringBitmap::from_iter([0, 3]));
    }

    #[test]
    fn test_effective_fragment_bitmap_no_fragment_bitmap() {
        let index = make_test_index(None, Some(RoaringBitmap::from_iter([1])));

        let existing_fragments = RoaringBitmap::from_iter([0, 1, 2]);

        // Should return None if no fragment_bitmap
        assert!(index
            .effective_fragment_bitmap(&existing_fragments)
            .is_none());
    }
}
