// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::io::Write;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::{collections::BTreeMap, io::Read};

use arrow_array::{Array, BinaryArray, GenericBinaryArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use byteorder::{ReadBytesExt, WriteBytesExt};
use deepsize::DeepSizeOf;
use roaring::{MultiOps, RoaringBitmap, RoaringTreemap};

use crate::Result;

use super::address::RowAddress;

mod nullable;

pub use nullable::{NullableRowAddrSet, NullableRowIdMask};

/// A mask that selects or deselects rows based on an allow-list or block-list.
#[derive(Clone, Debug, DeepSizeOf)]
pub enum RowIdMask {
    AllowList(RowAddrTreeMap),
    BlockList(RowAddrTreeMap),
}

impl Default for RowIdMask {
    fn default() -> Self {
        // Empty block list means all rows are allowed
        Self::BlockList(RowAddrTreeMap::new())
    }
}

impl RowIdMask {
    // Create a mask allowing all rows, this is an alias for [default]
    pub fn all_rows() -> Self {
        Self::default()
    }

    // Create a mask that doesn't allow anything
    pub fn allow_nothing() -> Self {
        Self::AllowList(RowAddrTreeMap::new())
    }

    // Create a mask from an allow list
    pub fn from_allowed(allow_list: RowAddrTreeMap) -> Self {
        Self::AllowList(allow_list)
    }

    // Create a mask from a block list
    pub fn from_block(block_list: RowAddrTreeMap) -> Self {
        Self::BlockList(block_list)
    }

    pub fn block_list(&self) -> Option<&RowAddrTreeMap> {
        match self {
            Self::BlockList(block_list) => Some(block_list),
            _ => None,
        }
    }

    pub fn allow_list(&self) -> Option<&RowAddrTreeMap> {
        match self {
            Self::AllowList(allow_list) => Some(allow_list),
            _ => None,
        }
    }

    /// True if the row_id is selected by the mask, false otherwise
    pub fn selected(&self, row_id: u64) -> bool {
        match self {
            Self::AllowList(allow_list) => allow_list.contains(row_id),
            Self::BlockList(block_list) => !block_list.contains(row_id),
        }
    }

    /// Return the indices of the input row ids that were valid
    pub fn selected_indices<'a>(&self, row_ids: impl Iterator<Item = &'a u64> + 'a) -> Vec<u64> {
        row_ids
            .enumerate()
            .filter_map(|(idx, row_id)| {
                if self.selected(*row_id) {
                    Some(idx as u64)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Also block the given ids
    pub fn also_block(self, block_list: RowAddrTreeMap) -> Self {
        match self {
            Self::AllowList(allow_list) => Self::AllowList(allow_list - block_list),
            Self::BlockList(existing) => Self::BlockList(existing | block_list),
        }
    }

    /// Also allow the given ids
    pub fn also_allow(self, allow_list: RowAddrTreeMap) -> Self {
        match self {
            Self::AllowList(existing) => Self::AllowList(existing | allow_list),
            Self::BlockList(block_list) => Self::BlockList(block_list - allow_list),
        }
    }

    /// Convert a mask into an arrow array
    ///
    /// A row id mask is not very arrow-compatible.  We can't make it a batch with
    /// two columns because the block list and allow list will have different lengths.  Also,
    /// there is no Arrow type for compressed bitmaps.
    ///
    /// However, we need to shove it into some kind of Arrow container to pass it along the
    /// datafusion stream.  Perhaps, in the future, we can add row id masks as first class
    /// types in datafusion, and this can be passed along as a mask / selection vector.
    ///
    /// We serialize this as a variable length binary array with two items.  The first item
    /// is the block list and the second item is the allow list.
    pub fn into_arrow(&self) -> Result<BinaryArray> {
        // NOTE: This serialization format must be stable as it is used in IPC.
        let (block_list, allow_list) = match self {
            Self::AllowList(allow_list) => (None, Some(allow_list)),
            Self::BlockList(block_list) => (Some(block_list), None),
        };

        let block_list_length = block_list
            .as_ref()
            .map(|bl| bl.serialized_size())
            .unwrap_or(0);
        let allow_list_length = allow_list
            .as_ref()
            .map(|al| al.serialized_size())
            .unwrap_or(0);
        let lengths = vec![block_list_length, allow_list_length];
        let offsets = OffsetBuffer::from_lengths(lengths);
        let mut value_bytes = vec![0; block_list_length + allow_list_length];
        let mut validity = vec![false, false];
        if let Some(block_list) = &block_list {
            validity[0] = true;
            block_list.serialize_into(&mut value_bytes[0..])?;
        }
        if let Some(allow_list) = &allow_list {
            validity[1] = true;
            allow_list.serialize_into(&mut value_bytes[block_list_length..])?;
        }
        let values = Buffer::from(value_bytes);
        let nulls = NullBuffer::from(validity);
        Ok(BinaryArray::try_new(offsets, values, Some(nulls))?)
    }

    /// Deserialize a row address mask from Arrow
    pub fn from_arrow(array: &GenericBinaryArray<i32>) -> Result<Self> {
        let block_list = if array.is_null(0) {
            None
        } else {
            Some(RowAddrTreeMap::deserialize_from(array.value(0)))
        }
        .transpose()?;

        let allow_list = if array.is_null(1) {
            None
        } else {
            Some(RowAddrTreeMap::deserialize_from(array.value(1)))
        }
        .transpose()?;

        let res = match (block_list, allow_list) {
            (Some(bl), None) => Self::BlockList(bl),
            (None, Some(al)) => Self::AllowList(al),
            (Some(block), Some(allow)) => Self::AllowList(allow).also_block(block),
            (None, None) => Self::all_rows(),
        };
        Ok(res)
    }

    /// Return the maximum number of row addresses that could be selected by this mask
    ///
    /// Will be None if this is a BlockList (unbounded)
    pub fn max_len(&self) -> Option<u64> {
        match self {
            Self::AllowList(selection) => selection.len(),
            Self::BlockList(_) => None,
        }
    }

    /// Iterate over the row addresses that are selected by the mask
    ///
    /// This is only possible if this is an AllowList and the maps don't contain
    /// any "full fragment" blocks.
    pub fn iter_ids(&self) -> Option<Box<dyn Iterator<Item = RowAddress> + '_>> {
        match self {
            Self::AllowList(allow_list) => {
                if let Some(allow_iter) = allow_list.row_addrs() {
                    Some(Box::new(allow_iter))
                } else {
                    None
                }
            }
            Self::BlockList(_) => None, // Can't iterate over block list
        }
    }
}

impl std::ops::Not for RowIdMask {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::AllowList(allow_list) => Self::BlockList(allow_list),
            Self::BlockList(block_list) => Self::AllowList(block_list),
        }
    }
}

impl std::ops::BitAnd for RowIdMask {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => Self::AllowList(a & b),
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => Self::AllowList(allow - block),
            (Self::BlockList(a), Self::BlockList(b)) => Self::BlockList(a | b),
        }
    }
}

impl std::ops::BitOr for RowIdMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => Self::AllowList(a | b),
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => Self::BlockList(block - allow),
            (Self::BlockList(a), Self::BlockList(b)) => Self::BlockList(a & b),
        }
    }
}

/// A collection of row addresses.
///
/// Note: For stable row id mode, this may be split into a separate structure in the future.
///
/// These row ids may either be stable-style (where they can be an incrementing
/// u64 sequence) or address style, where they are a fragment id and a row offset.
/// When address style, this supports setting entire fragments as selected,
/// without needing to enumerate all the ids in the fragment.
///
/// This is similar to a [RoaringTreemap] but it is optimized for the case where
/// entire fragments are selected or deselected.
#[derive(Clone, Debug, Default, PartialEq, DeepSizeOf)]
pub struct RowAddrTreeMap {
    /// The contents of the set. If there is a pair (k, Full) then the entire
    /// fragment k is selected. If there is a pair (k, Partial(v)) then the
    /// fragment k has the selected rows in v.
    inner: BTreeMap<u32, RowAddrSelection>,
}

#[derive(Clone, Debug, PartialEq)]
enum RowAddrSelection {
    Full,
    Partial(RoaringBitmap),
}

impl DeepSizeOf for RowAddrSelection {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        match self {
            Self::Full => 0,
            Self::Partial(bitmap) => bitmap.serialized_size(),
        }
    }
}

impl RowAddrSelection {
    fn union_all(selections: &[&Self]) -> Self {
        let mut is_full = false;

        let res = Self::Partial(
            selections
                .iter()
                .filter_map(|selection| match selection {
                    Self::Full => {
                        is_full = true;
                        None
                    }
                    Self::Partial(bitmap) => Some(bitmap),
                })
                .union(),
        );

        if is_full {
            Self::Full
        } else {
            res
        }
    }
}

impl RowAddrTreeMap {
    /// Create an empty set
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The number of rows in the map
    ///
    /// If there are any "full fragment" items then this is unknown and None is returned
    pub fn len(&self) -> Option<u64> {
        self.inner
            .values()
            .map(|row_addr_selection| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(indices) => Some(indices.len()),
            })
            .try_fold(0_u64, |acc, next| next.map(|next| next + acc))
    }

    /// An iterator of row addrs
    ///
    /// If there are any "full fragment" items then this can't be calculated and None
    /// is returned
    pub fn row_addrs(&self) -> Option<impl Iterator<Item = RowAddress> + '_> {
        let inner_iters = self
            .inner
            .iter()
            .filter_map(|(frag_id, row_addr_selection)| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(bitmap) => Some(
                    bitmap
                        .iter()
                        .map(|row_offset| RowAddress::new_from_parts(*frag_id, row_offset)),
                ),
            })
            .collect::<Vec<_>>();
        if inner_iters.len() != self.inner.len() {
            None
        } else {
            Some(inner_iters.into_iter().flatten())
        }
    }

    /// Insert a single value into the set
    ///
    /// Returns true if the value was not already in the set.
    ///
    /// ```rust
    /// use lance_core::utils::mask::RowAddrTreeMap;
    ///
    /// let mut set = RowAddrTreeMap::new();
    /// assert_eq!(set.insert(10), true);
    /// assert_eq!(set.insert(10), false);
    /// assert_eq!(set.contains(10), true);
    /// ```
    pub fn insert(&mut self, value: u64) -> bool {
        let fragment = (value >> 32) as u32;
        let row_addr = value as u32;
        match self.inner.get_mut(&fragment) {
            None => {
                let mut set = RoaringBitmap::new();
                set.insert(row_addr);
                self.inner.insert(fragment, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Full) => false,
            Some(RowAddrSelection::Partial(set)) => set.insert(row_addr),
        }
    }

    /// Insert a range of values into the set
    pub fn insert_range<R: RangeBounds<u64>>(&mut self, range: R) -> u64 {
        // Separate the start and end into high and low bits.
        let (mut start_high, mut start_low) = match range.start_bound() {
            std::ops::Bound::Included(&start) => ((start >> 32) as u32, start as u32),
            std::ops::Bound::Excluded(&start) => {
                let start = start.saturating_add(1);
                ((start >> 32) as u32, start as u32)
            }
            std::ops::Bound::Unbounded => (0, 0),
        };

        let (end_high, end_low) = match range.end_bound() {
            std::ops::Bound::Included(&end) => ((end >> 32) as u32, end as u32),
            std::ops::Bound::Excluded(&end) => {
                let end = end.saturating_sub(1);
                ((end >> 32) as u32, end as u32)
            }
            std::ops::Bound::Unbounded => (u32::MAX, u32::MAX),
        };

        let mut count = 0;

        while start_high <= end_high {
            let start = start_low;
            let end = if start_high == end_high {
                end_low
            } else {
                u32::MAX
            };
            let fragment = start_high;
            match self.inner.get_mut(&fragment) {
                None => {
                    let mut set = RoaringBitmap::new();
                    count += set.insert_range(start..=end);
                    self.inner.insert(fragment, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {}
                Some(RowAddrSelection::Partial(set)) => {
                    count += set.insert_range(start..=end);
                }
            }
            start_high += 1;
            start_low = 0;
        }

        count
    }

    /// Add a bitmap for a single fragment
    pub fn insert_bitmap(&mut self, fragment: u32, bitmap: RoaringBitmap) {
        self.inner
            .insert(fragment, RowAddrSelection::Partial(bitmap));
    }

    /// Add a whole fragment to the set
    pub fn insert_fragment(&mut self, fragment_id: u32) {
        self.inner.insert(fragment_id, RowAddrSelection::Full);
    }

    pub fn get_fragment_bitmap(&self, fragment_id: u32) -> Option<&RoaringBitmap> {
        match self.inner.get(&fragment_id) {
            None => None,
            Some(RowAddrSelection::Full) => None,
            Some(RowAddrSelection::Partial(set)) => Some(set),
        }
    }

    /// Returns whether the set contains the given value
    pub fn contains(&self, value: u64) -> bool {
        let upper = (value >> 32) as u32;
        let lower = value as u32;
        match self.inner.get(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => true,
            Some(RowAddrSelection::Partial(fragment_set)) => fragment_set.contains(lower),
        }
    }

    pub fn remove(&mut self, value: u64) -> bool {
        let upper = (value >> 32) as u32;
        let lower = value as u32;
        match self.inner.get_mut(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => {
                let mut set = RoaringBitmap::full();
                set.remove(lower);
                self.inner.insert(upper, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Partial(lower_set)) => {
                let removed = lower_set.remove(lower);
                if lower_set.is_empty() {
                    self.inner.remove(&upper);
                }
                removed
            }
        }
    }

    pub fn retain_fragments(&mut self, frag_ids: impl IntoIterator<Item = u32>) {
        let frag_id_set = frag_ids.into_iter().collect::<HashSet<_>>();
        self.inner
            .retain(|frag_id, _| frag_id_set.contains(frag_id));
    }

    /// Compute the serialized size of the set.
    pub fn serialized_size(&self) -> usize {
        // Starts at 4 because of the u32 num_entries
        let mut size = 4;
        for set in self.inner.values() {
            // Each entry is 8 bytes for the fragment id and the bitmap size
            size += 8;
            if let RowAddrSelection::Partial(set) = set {
                size += set.serialized_size();
            }
        }
        size
    }

    /// Serialize the set into the given buffer
    ///
    /// The serialization format is stable and used for index serialization
    ///
    /// The serialization format is:
    /// * u32: num_entries
    ///
    /// for each entry:
    ///   * u32: fragment_id
    ///   * u32: bitmap size
    ///   * \[u8\]: bitmap
    ///
    /// If bitmap size is zero then the entire fragment is selected.
    pub fn serialize_into<W: Write>(&self, mut writer: W) -> Result<()> {
        writer.write_u32::<byteorder::LittleEndian>(self.inner.len() as u32)?;
        for (fragment, set) in &self.inner {
            writer.write_u32::<byteorder::LittleEndian>(*fragment)?;
            if let RowAddrSelection::Partial(set) = set {
                writer.write_u32::<byteorder::LittleEndian>(set.serialized_size() as u32)?;
                set.serialize_into(&mut writer)?;
            } else {
                writer.write_u32::<byteorder::LittleEndian>(0)?;
            }
        }
        Ok(())
    }

    /// Deserialize the set from the given buffer
    pub fn deserialize_from<R: Read>(mut reader: R) -> Result<Self> {
        let num_entries = reader.read_u32::<byteorder::LittleEndian>()?;
        let mut inner = BTreeMap::new();
        for _ in 0..num_entries {
            let fragment = reader.read_u32::<byteorder::LittleEndian>()?;
            let bitmap_size = reader.read_u32::<byteorder::LittleEndian>()?;
            if bitmap_size == 0 {
                inner.insert(fragment, RowAddrSelection::Full);
            } else {
                let mut buffer = vec![0; bitmap_size as usize];
                reader.read_exact(&mut buffer)?;
                let set = RoaringBitmap::deserialize_from(&buffer[..])?;
                inner.insert(fragment, RowAddrSelection::Partial(set));
            }
        }
        Ok(Self { inner })
    }

    pub fn union_all(maps: &[&Self]) -> Self {
        let mut new_map = BTreeMap::new();

        for map in maps {
            for (fragment, selection) in &map.inner {
                new_map
                    .entry(fragment)
                    // I hate this allocation, but I can't think of a better way
                    .or_insert_with(|| Vec::with_capacity(maps.len()))
                    .push(selection);
            }
        }

        let new_map = new_map
            .into_iter()
            .map(|(&fragment, selections)| (fragment, RowAddrSelection::union_all(&selections)))
            .collect();

        Self { inner: new_map }
    }

    /// Apply a mask to the row ids
    ///
    /// For AllowList: only keep rows that are in the selection and not null
    /// For BlockList: remove rows that are blocked (not null) and remove nulls
    pub fn mask(&mut self, mask: &RowIdMask) {
        match mask {
            RowIdMask::AllowList(allow_list) => {
                *self &= allow_list;
            }
            RowIdMask::BlockList(block_list) => {
                *self -= block_list;
            }
        }
    }

    /// Convert the set into an iterator of row addrs
    ///
    /// # Safety
    ///
    /// This is unsafe because if any of the inner RowAddrSelection elements
    /// is not a Partial then the iterator will panic because we don't know
    /// the size of the bitmap.
    pub unsafe fn into_addr_iter(self) -> impl Iterator<Item = u64> {
        self.inner
            .into_iter()
            .flat_map(|(fragment, selection)| match selection {
                RowAddrSelection::Full => panic!("Size of full fragment is unknown"),
                RowAddrSelection::Partial(bitmap) => bitmap.into_iter().map(move |val| {
                    let fragment = fragment as u64;
                    let row_offset = val as u64;
                    (fragment << 32) | row_offset
                }),
            })
    }
}

impl std::ops::BitOr<Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl std::ops::BitOr<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitor(mut self, rhs: &Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl std::ops::BitOrAssign<Self> for RowAddrTreeMap {
    fn bitor_assign(&mut self, rhs: Self) {
        *self |= &rhs;
    }
}

impl std::ops::BitOrAssign<&Self> for RowAddrTreeMap {
    fn bitor_assign(&mut self, rhs: &Self) {
        for (fragment, rhs_set) in &rhs.inner {
            let lhs_set = self.inner.get_mut(fragment);
            if let Some(lhs_set) = lhs_set {
                match lhs_set {
                    RowAddrSelection::Full => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    RowAddrSelection::Partial(lhs_bitmap) => match rhs_set {
                        RowAddrSelection::Full => {
                            *lhs_set = RowAddrSelection::Full;
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_bitmap |= rhs_set;
                        }
                    },
                }
            } else {
                self.inner.insert(*fragment, rhs_set.clone());
            }
        }
    }
}

impl std::ops::BitAnd<Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self &= &rhs;
        self
    }
}

impl std::ops::BitAnd<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitand(mut self, rhs: &Self) -> Self::Output {
        self &= rhs;
        self
    }
}

impl std::ops::BitAndAssign<Self> for RowAddrTreeMap {
    fn bitand_assign(&mut self, rhs: Self) {
        *self &= &rhs;
    }
}

impl std::ops::BitAndAssign<&Self> for RowAddrTreeMap {
    fn bitand_assign(&mut self, rhs: &Self) {
        // Remove fragment that aren't on the RHS
        self.inner
            .retain(|fragment, _| rhs.inner.contains_key(fragment));

        // For fragments that are on the RHS, intersect the bitmaps
        for (fragment, mut lhs_set) in &mut self.inner {
            match (&mut lhs_set, rhs.inner.get(fragment)) {
                (_, None) => {} // Already handled by retain
                (_, Some(RowAddrSelection::Full)) => {
                    // Everything selected on RHS, so can leave LHS untouched.
                }
                (RowAddrSelection::Partial(lhs_set), Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set &= rhs_set;
                }
                (RowAddrSelection::Full, Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set = RowAddrSelection::Partial(rhs_set.clone());
                }
            }
        }
        // Some bitmaps might now be empty. If they are, we should remove them.
        self.inner.retain(|_, set| match set {
            RowAddrSelection::Partial(set) => !set.is_empty(),
            RowAddrSelection::Full => true,
        });
    }
}

impl std::ops::Sub<Self> for RowAddrTreeMap {
    type Output = Self;

    fn sub(mut self, rhs: Self) -> Self {
        self -= &rhs;
        self
    }
}

impl std::ops::Sub<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn sub(mut self, rhs: &Self) -> Self {
        self -= rhs;
        self
    }
}

impl std::ops::SubAssign<&Self> for RowAddrTreeMap {
    fn sub_assign(&mut self, rhs: &Self) {
        for (fragment, rhs_set) in &rhs.inner {
            match self.inner.get_mut(fragment) {
                None => {}
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                    match rhs_set {
                        RowAddrSelection::Full => {
                            self.inner.remove(fragment);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            // This generally won't be hit.
                            let mut set = RoaringBitmap::full();
                            set -= rhs_set;
                            self.inner.insert(*fragment, RowAddrSelection::Partial(set));
                        }
                    }
                }
                Some(RowAddrSelection::Partial(lhs_set)) => match rhs_set {
                    RowAddrSelection::Full => {
                        self.inner.remove(fragment);
                    }
                    RowAddrSelection::Partial(rhs_set) => {
                        *lhs_set -= rhs_set;
                        if lhs_set.is_empty() {
                            self.inner.remove(fragment);
                        }
                    }
                },
            }
        }
    }
}

impl FromIterator<u64> for RowAddrTreeMap {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        let mut inner = BTreeMap::new();
        for row_id in iter {
            let upper = (row_id >> 32) as u32;
            let lower = row_id as u32;
            match inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
        Self { inner }
    }
}

impl<'a> FromIterator<&'a u64> for RowAddrTreeMap {
    fn from_iter<T: IntoIterator<Item = &'a u64>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().copied())
    }
}

impl From<Range<u64>> for RowAddrTreeMap {
    fn from(range: Range<u64>) -> Self {
        let mut map = Self::default();
        map.insert_range(range);
        map
    }
}

impl From<RangeInclusive<u64>> for RowAddrTreeMap {
    fn from(range: RangeInclusive<u64>) -> Self {
        let mut map = Self::default();
        map.insert_range(range);
        map
    }
}

impl From<RoaringTreemap> for RowAddrTreeMap {
    fn from(roaring: RoaringTreemap) -> Self {
        let mut inner = BTreeMap::new();
        for (fragment, set) in roaring.bitmaps() {
            inner.insert(fragment, RowAddrSelection::Partial(set.clone()));
        }
        Self { inner }
    }
}

impl Extend<u64> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = u64>>(&mut self, iter: T) {
        for row_id in iter {
            let upper = (row_id >> 32) as u32;
            let lower = row_id as u32;
            match self.inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    self.inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
    }
}

impl<'a> Extend<&'a u64> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = &'a u64>>(&mut self, iter: T) {
        self.extend(iter.into_iter().copied())
    }
}

// Extending with RowAddrTreeMap is basically a cumulative set union
impl Extend<Self> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        for other in iter {
            for (fragment, set) in other.inner {
                match self.inner.get_mut(&fragment) {
                    None => {
                        self.inner.insert(fragment, set);
                    }
                    Some(RowAddrSelection::Full) => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    Some(RowAddrSelection::Partial(lhs_set)) => match set {
                        RowAddrSelection::Full => {
                            self.inner.insert(fragment, RowAddrSelection::Full);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_set |= rhs_set;
                        }
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prop_assert_eq;

    #[test]
    fn test_ops() {
        let mask = RowIdMask::default();
        assert!(mask.selected(1));
        assert!(mask.selected(5));
        let block_list = mask.also_block(RowAddrTreeMap::from_iter(&[0, 5, 15]));
        assert!(block_list.selected(1));
        assert!(!block_list.selected(5));
        let allow_list = RowIdMask::from_allowed(RowAddrTreeMap::from_iter(&[0, 2, 5]));
        assert!(!allow_list.selected(1));
        assert!(allow_list.selected(5));
        let combined = block_list & allow_list;
        assert!(combined.selected(2));
        assert!(!combined.selected(0));
        assert!(!combined.selected(5));
        let other = RowIdMask::from_allowed(RowAddrTreeMap::from_iter(&[3]));
        let combined = combined | other;
        assert!(combined.selected(2));
        assert!(combined.selected(3));
        assert!(!combined.selected(0));
        assert!(!combined.selected(5));

        let block_list = RowIdMask::from_block(RowAddrTreeMap::from_iter(&[0]));
        let allow_list = RowIdMask::from_allowed(RowAddrTreeMap::from_iter(&[3]));
        let combined = block_list | allow_list;
        assert!(combined.selected(1));
    }

    #[test]
    fn test_logical_or() {
        let allow1 = RowIdMask::from_allowed(RowAddrTreeMap::from_iter(&[5, 6, 7, 8, 9]));
        let block1 = RowIdMask::from_block(RowAddrTreeMap::from_iter(&[5, 6]));
        let mixed1 = allow1
            .clone()
            .also_block(RowAddrTreeMap::from_iter(&[5, 6]));
        let allow2 = RowIdMask::from_allowed(RowAddrTreeMap::from_iter(&[2, 3, 4, 5, 6, 7, 8]));
        let block2 = RowIdMask::from_block(RowAddrTreeMap::from_iter(&[4, 5]));
        let mixed2 = allow2
            .clone()
            .also_block(RowAddrTreeMap::from_iter(&[4, 5]));

        fn check(lhs: &RowIdMask, rhs: &RowIdMask, expected: &[u64]) {
            for mask in [lhs.clone() | rhs.clone(), rhs.clone() | lhs.clone()] {
                let values = (0..10)
                    .filter(|val| mask.selected(*val))
                    .collect::<Vec<_>>();
                assert_eq!(&values, expected);
            }
        }

        check(&allow1, &allow1, &[5, 6, 7, 8, 9]);
        check(&block1, &block1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&mixed1, &mixed1, &[7, 8, 9]);
        check(&allow2, &allow2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed2, &mixed2, &[2, 3, 6, 7, 8]);

        check(&allow1, &block1, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed1, &[5, 6, 7, 8, 9]);
        check(&allow1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &block2, &[0, 1, 2, 3, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed2, &[2, 3, 5, 6, 7, 8, 9]);
        check(&block1, &mixed1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&block1, &allow2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&block1, &block2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&block1, &mixed2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&mixed1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&mixed1, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed1, &mixed2, &[2, 3, 6, 7, 8, 9]);
        check(&allow2, &block2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow2, &mixed2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &mixed2, &[0, 1, 2, 3, 6, 7, 8, 9]);
    }

    #[test]
    fn test_deserialize_legacy_format() {
        // Test that we can deserialize the old format where both allow_list
        // and block_list could be present in the serialized form.
        //
        // The old format (before this PR) used a struct with both allow_list and block_list
        // fields. The new format uses an enum. The deserialization code should handle
        // the case where both lists are present by converting to AllowList(allow - block).

        // Create the RowIdTreeMaps and serialize them directly
        let allow = RowAddrTreeMap::from_iter(&[1, 2, 3, 4, 5, 10, 15]);
        let block = RowAddrTreeMap::from_iter(&[2, 4, 15]);

        // Serialize using the stable RowIdTreeMap serialization format
        let block_bytes = {
            let mut buf = Vec::with_capacity(block.serialized_size());
            block.serialize_into(&mut buf).unwrap();
            buf
        };
        let allow_bytes = {
            let mut buf = Vec::with_capacity(allow.serialized_size());
            allow.serialize_into(&mut buf).unwrap();
            buf
        };

        // Construct a binary array with both values present (simulating old format)
        let old_format_array =
            BinaryArray::from_opt_vec(vec![Some(&block_bytes), Some(&allow_bytes)]);

        // Deserialize - should handle this by creating AllowList(allow - block)
        let deserialized = RowIdMask::from_arrow(&old_format_array).unwrap();

        // The expected result: AllowList([1, 2, 3, 4, 5, 10, 15] - [2, 4, 15]) = [1, 3, 5, 10]
        let expected_rows = vec![1u64, 3, 5, 10];
        for row in &expected_rows {
            assert!(
                deserialized.selected(*row),
                "Row {} should be selected",
                row
            );
        }

        // Verify blocked rows are not selected
        assert!(!deserialized.selected(2), "Row 2 should be blocked");
        assert!(!deserialized.selected(4), "Row 4 should be blocked");
        assert!(!deserialized.selected(15), "Row 15 should be blocked");

        // Verify it's an AllowList variant
        assert!(
            deserialized.allow_list().is_some(),
            "Should deserialize to AllowList variant"
        );
    }

    #[test]
    fn test_deserialize_legacy_empty_lists() {
        // Test edge cases with None values in old format

        // Case 1: Both None (should become all_rows)
        let array = BinaryArray::from_opt_vec(vec![None, None]);
        let mask = RowIdMask::from_arrow(&array).unwrap();
        assert!(mask.selected(0));
        assert!(mask.selected(100));
        assert!(mask.selected(u64::MAX));

        // Case 2: Only block list (no allow list)
        let block = RowAddrTreeMap::from_iter(&[5, 10]);
        let block_bytes = {
            let mut buf = Vec::with_capacity(block.serialized_size());
            block.serialize_into(&mut buf).unwrap();
            buf
        };
        let array = BinaryArray::from_opt_vec(vec![Some(&block_bytes[..]), None]);
        let mask = RowIdMask::from_arrow(&array).unwrap();
        assert!(mask.selected(0));
        assert!(!mask.selected(5));
        assert!(!mask.selected(10));
        assert!(mask.selected(15));

        // Case 3: Only allow list (no block list)
        let allow = RowAddrTreeMap::from_iter(&[5, 10]);
        let allow_bytes = {
            let mut buf = Vec::with_capacity(allow.serialized_size());
            allow.serialize_into(&mut buf).unwrap();
            buf
        };
        let array = BinaryArray::from_opt_vec(vec![None, Some(&allow_bytes[..])]);
        let mask = RowIdMask::from_arrow(&array).unwrap();
        assert!(!mask.selected(0));
        assert!(mask.selected(5));
        assert!(mask.selected(10));
        assert!(!mask.selected(15));
    }

    #[test]
    fn test_map_insert_range() {
        let ranges = &[
            (0..10),
            (40..500),
            ((u32::MAX as u64 - 10)..(u32::MAX as u64 + 20)),
        ];

        for range in ranges {
            let mut mask = RowAddrTreeMap::default();

            let count = mask.insert_range(range.clone());
            let expected = range.end - range.start;
            assert_eq!(count, expected);

            let count = mask.insert_range(range.clone());
            assert_eq!(count, 0);

            let new_range = range.start + 5..range.end + 5;
            let count = mask.insert_range(new_range.clone());
            assert_eq!(count, 5);
        }

        let mut mask = RowAddrTreeMap::default();
        let count = mask.insert_range(..10);
        assert_eq!(count, 10);
        assert!(mask.contains(0));

        let count = mask.insert_range(20..=24);
        assert_eq!(count, 5);

        mask.insert_fragment(0);
        let count = mask.insert_range(100..200);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_map_remove() {
        let mut mask = RowAddrTreeMap::default();

        assert!(!mask.remove(20));

        mask.insert(20);
        assert!(mask.contains(20));
        assert!(mask.remove(20));
        assert!(!mask.contains(20));

        mask.insert_range(10..=20);
        assert!(mask.contains(15));
        assert!(mask.remove(15));
        assert!(!mask.contains(15));

        // We don't test removing from a full fragment, because that would take
        // a lot of memory.
    }

    proptest::proptest! {
        #[test]
        fn test_map_serialization_roundtrip(
            values in proptest::collection::vec(
                (0..u32::MAX, proptest::option::of(proptest::collection::vec(0..u32::MAX, 0..1000))),
                0..10
            )
        ) {
            let mut mask = RowAddrTreeMap::default();
            for (fragment, rows) in values {
                if let Some(rows) = rows {
                    let bitmap = RoaringBitmap::from_iter(rows);
                    mask.insert_bitmap(fragment, bitmap);
                } else {
                    mask.insert_fragment(fragment);
                }
            }

            let mut data = Vec::new();
            mask.serialize_into(&mut data).unwrap();
            let deserialized = RowAddrTreeMap::deserialize_from(data.as_slice()).unwrap();
            prop_assert_eq!(mask, deserialized);
        }

        #[test]
        fn test_map_intersect(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowAddrTreeMap::default();
            for fragment in &left_full_fragments {
                if right_full_fragments.contains(fragment) {
                    expected.insert_fragment(*fragment);
                }
            }

            let left_in_right = left_rows.iter().filter(|row| {
                right_rows.contains(row)
                    || right_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(left_in_right);
            let right_in_left = right_rows.iter().filter(|row| {
                left_rows.contains(row)
                    || left_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(right_in_left);

            let actual = left & right;
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_union(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                expected.insert_fragment(fragment);
            }
            for fragment in right_full_fragments {
                expected.insert_fragment(fragment);
            }

            let combined_rows = left_rows.iter().chain(right_rows.iter());
            expected.extend(combined_rows);

            let actual = left | right;
            for actual_key_val in &actual.inner {
                proptest::prop_assert!(expected.inner.contains_key(actual_key_val.0));
                let expected_val = expected.inner.get(actual_key_val.0).unwrap();
                prop_assert_eq!(
                    actual_key_val.1,
                    expected_val,
                    "error on key {}",
                    actual_key_val.0
                );
            }
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_subassign_rows(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            right.extend(right_rows.iter().copied());

            let mut expected = left.clone();
            for row in right_rows {
                expected.remove(row);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

        #[test]
        fn test_map_subassign_frags(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }

            let mut expected = left.clone();
            for fragment in right_full_fragments {
                expected.inner.remove(&fragment);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

    }
}
