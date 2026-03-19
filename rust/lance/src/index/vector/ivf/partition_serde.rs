// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization and zero-copy deserialization for IVF partition cache entries.
//!
//! The format is a simple binary layout designed for ephemeral caching (not stable across versions):
//!
//! ```text
//! [header_len: u64 LE]
//! [header: JSON bytes]
//! [sub_index IPC file bytes]
//! [... quantizer-specific IPC sections ...]
//! [storage batch IPC file bytes]
//! ```
//!
//! Each IPC section is a complete Arrow IPC file. On deserialization, the IPC
//! sections are read zero-copy using [`FileDecoder`] so that Arrow arrays
//! reference the original buffer directly.

use std::sync::Arc;

use arrow_array::{FixedSizeListArray, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::{FileDecoder, read_footer_length};
use arrow_ipc::root_as_footer;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use lance_core::{Error, Result};
use lance_index::vector::bq::RQRotationType;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::storage::RabitQuantizationMetadata;
use lance_index::vector::flat::index::{FlatMetadata, FlatQuantizer};
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::pq::storage::ProductQuantizationMetadata;
use lance_index::vector::quantizer::{Quantization, QuantizerStorage};
use lance_index::vector::sq::ScalarQuantizer;
use lance_index::vector::sq::storage::ScalarQuantizationMetadata;
use lance_index::vector::storage::VectorStore;
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;
use serde::{Deserialize, Serialize};

use super::v2::PartitionEntry;

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------

fn distance_type_to_u8(dt: DistanceType) -> u8 {
    match dt {
        DistanceType::L2 => 0,
        DistanceType::Cosine => 1,
        DistanceType::Dot => 2,
        DistanceType::Hamming => 3,
    }
}

fn u8_to_distance_type(v: u8) -> Result<DistanceType> {
    match v {
        0 => Ok(DistanceType::L2),
        1 => Ok(DistanceType::Cosine),
        2 => Ok(DistanceType::Dot),
        3 => Ok(DistanceType::Hamming),
        _ => Err(Error::io(format!("unknown distance type: {v}"))),
    }
}

fn rotation_type_to_u8(rt: RQRotationType) -> u8 {
    match rt {
        RQRotationType::Matrix => 0,
        RQRotationType::Fast => 1,
    }
}

fn u8_to_rotation_type(v: u8) -> Result<RQRotationType> {
    match v {
        0 => Ok(RQRotationType::Matrix),
        1 => Ok(RQRotationType::Fast),
        _ => Err(Error::io(format!("unknown rotation type: {v}"))),
    }
}

/// Write one or more RecordBatches as a complete Arrow IPC file into a Vec<u8>.
///
/// Panics if `batches` is empty (caller is responsible for checking).
fn write_ipc_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, batches[0].schema_ref())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Write a single RecordBatch as a complete Arrow IPC file into a Vec<u8>.
fn write_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    write_ipc_batches(std::slice::from_ref(batch))
}

/// Decode the IPC footer and schema from a `Buffer`, returning the decoder and
/// the list of record-batch blocks. Zero-copy: all returned data references
/// the original buffer.
fn parse_ipc_footer(data: &Buffer) -> Result<(FileDecoder, Vec<arrow_ipc::Block>)> {
    let trailer_start = data
        .len()
        .checked_sub(10)
        .ok_or_else(|| Error::io("IPC section too small to contain footer".to_string()))?;
    let footer_len = read_footer_length(
        data[trailer_start..]
            .try_into()
            .map_err(|_| Error::io("IPC section too small for footer length".to_string()))?,
    )?;
    let footer_start = trailer_start
        .checked_sub(footer_len)
        .ok_or_else(|| Error::io("IPC footer length exceeds section size".to_string()))?;
    let footer = root_as_footer(&data[footer_start..trailer_start])
        .map_err(|e| Error::io(format!("failed to parse IPC footer: {e}")))?;

    let schema =
        Arc::new(fb_to_schema(footer.schema().ok_or_else(|| {
            Error::io("IPC footer missing schema".to_string())
        })?));

    let mut decoder = FileDecoder::new(schema, footer.version());

    for block in footer.dictionaries().iter().flatten() {
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let block_data = data.slice_with_length(block.offset() as usize, block_len);
        decoder.read_dictionary(block, &block_data)?;
    }

    let batch_blocks: Vec<arrow_ipc::Block> = footer
        .recordBatches()
        .map(|b| b.iter().copied().collect())
        .unwrap_or_default();

    Ok((decoder, batch_blocks))
}

/// Read all RecordBatches from an Arrow IPC file stored in a `Buffer`, zero-copy.
///
/// The returned arrays reference slices of the provided buffer directly.
fn read_ipc_all_zero_copy(data: Buffer) -> Result<Vec<RecordBatch>> {
    let (decoder, batch_blocks) = parse_ipc_footer(&data)?;
    batch_blocks
        .iter()
        .map(|block| {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let block_data = data.slice_with_length(block.offset() as usize, block_len);
            decoder
                .read_record_batch(block, &block_data)?
                .ok_or_else(|| Error::io("IPC record batch was None".to_string()))
        })
        .collect()
}

/// Read a single RecordBatch from an Arrow IPC file stored in a `Buffer`, zero-copy.
///
/// The returned `RecordBatch`'s arrays reference slices of the provided buffer
/// directly, avoiding copies.
fn read_ipc_zero_copy(data: Buffer) -> Result<RecordBatch> {
    let (decoder, batch_blocks) = parse_ipc_footer(&data)?;
    if batch_blocks.is_empty() {
        return Err(Error::io("IPC file contains no record batches".to_string()));
    }
    let block = &batch_blocks[0];
    let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
    let block_data = data.slice_with_length(block.offset() as usize, block_len);
    decoder
        .read_record_batch(block, &block_data)?
        .ok_or_else(|| Error::io("IPC record batch was None".to_string()))
}

/// Wrap a `FixedSizeListArray` in a single-column RecordBatch with the given column name.
fn fsl_to_batch(arr: &FixedSizeListArray, name: &str) -> Result<RecordBatch> {
    let field = Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", arr.value_type(), true)),
            arr.value_length(),
        ),
        false,
    );
    let schema = Arc::new(Schema::new(vec![field]));
    Ok(RecordBatch::try_new(schema, vec![Arc::new(arr.clone())])?)
}

/// Extract a `FixedSizeListArray` from the first column of a RecordBatch.
fn batch_to_fsl(batch: &RecordBatch) -> Result<FixedSizeListArray> {
    let col = batch.column(0);
    col.as_any()
        .downcast_ref::<FixedSizeListArray>()
        .cloned()
        .ok_or_else(|| Error::io("column is not FixedSizeListArray".to_string()))
}

fn codebook_to_batch(codebook: &FixedSizeListArray) -> Result<RecordBatch> {
    fsl_to_batch(codebook, "codebook")
}

fn batch_to_codebook(batch: &RecordBatch) -> Result<FixedSizeListArray> {
    batch_to_fsl(batch)
}

// ---------------------------------------------------------------------------
// PQ
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PqPartitionHeader {
    distance_type: u8,
    nbits: u32,
    num_sub_vectors: usize,
    dimension: usize,
    transposed: bool,
    /// Length of the sub-index IPC section in bytes.
    sub_index_len: u64,
    /// Length of the codebook IPC section in bytes.
    codebook_len: u64,
    /// Length of the storage batch IPC section in bytes.
    storage_len: u64,
}

impl<S: IvfSubIndex> PartitionEntry<S, ProductQuantizer> {
    /// Serialize this partition entry to bytes.
    ///
    /// The sub-index, PQ codebook, and storage batch are each written as Arrow
    /// IPC file sections, preceded by a small JSON header containing scalar
    /// metadata and section lengths.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let metadata = self.storage.metadata();
        let distance_type = self.storage.distance_type();

        // Serialize the three Arrow sections.
        let sub_index_ipc = write_ipc(&self.index.to_batch()?)?;
        let codebook = metadata.codebook.as_ref().ok_or_else(|| {
            Error::io("PQ metadata missing codebook during serialization".to_string())
        })?;
        let codebook_ipc = write_ipc(&codebook_to_batch(codebook)?)?;
        let storage_batches: Vec<_> = self.storage.to_batches()?.collect();
        let storage_ipc = if storage_batches.len() == 1 {
            write_ipc(&storage_batches[0])?
        } else {
            return Err(Error::io(
                "expected exactly one storage batch for PQ storage".to_string(),
            ));
        };

        let header = PqPartitionHeader {
            distance_type: distance_type_to_u8(distance_type),
            nbits: metadata.nbits,
            num_sub_vectors: metadata.num_sub_vectors,
            dimension: metadata.dimension,
            transposed: metadata.transposed,
            sub_index_len: sub_index_ipc.len() as u64,
            codebook_len: codebook_ipc.len() as u64,
            storage_len: storage_ipc.len() as u64,
        };

        let header_json = serde_json::to_vec(&header)?;

        let total_len =
            8 + header_json.len() + sub_index_ipc.len() + codebook_ipc.len() + storage_ipc.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(header_json.len() as u64).to_le_bytes());
        out.extend_from_slice(&header_json);
        out.extend_from_slice(&sub_index_ipc);
        out.extend_from_slice(&codebook_ipc);
        out.extend_from_slice(&storage_ipc);

        Ok(out)
    }

    /// Deserialize a partition entry from bytes, zero-copy for Arrow data.
    ///
    /// The Arrow IPC sections are decoded using [`FileDecoder`] so that the
    /// resulting arrays reference slices of the provided `Bytes` buffer directly.
    pub fn deserialize(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::io("partition data too small".to_string()));
        }

        let header_len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let header_end = 8 + header_len;
        if data.len() < header_end {
            return Err(Error::io("partition data truncated in header".to_string()));
        }

        let header: PqPartitionHeader = serde_json::from_slice(&data[8..header_end])?;
        let distance_type = u8_to_distance_type(header.distance_type)?;

        let sub_index_start = header_end;
        let sub_index_end = sub_index_start + header.sub_index_len as usize;
        let codebook_start = sub_index_end;
        let codebook_end = codebook_start + header.codebook_len as usize;
        let storage_start = codebook_end;
        let storage_end = storage_start + header.storage_len as usize;

        if data.len() < storage_end {
            return Err(Error::io(
                "partition data truncated in IPC sections".to_string(),
            ));
        }

        // Zero-copy: create Buffer slices backed by the original Bytes.
        let buffer = Buffer::from(data);
        let sub_index_buf =
            buffer.slice_with_length(sub_index_start, header.sub_index_len as usize);
        let codebook_buf = buffer.slice_with_length(codebook_start, header.codebook_len as usize);
        let storage_buf = buffer.slice_with_length(storage_start, header.storage_len as usize);

        let sub_index_batch = read_ipc_zero_copy(sub_index_buf)?;
        let codebook_batch = read_ipc_zero_copy(codebook_buf)?;
        let storage_batch = read_ipc_zero_copy(storage_buf)?;

        let index = S::load(sub_index_batch)?;
        let codebook = batch_to_codebook(&codebook_batch)?;

        let metadata = ProductQuantizationMetadata {
            codebook_position: 0,
            nbits: header.nbits,
            num_sub_vectors: header.num_sub_vectors,
            dimension: header.dimension,
            codebook: Some(codebook),
            codebook_tensor: Vec::new(),
            transposed: header.transposed,
        };

        let storage = <ProductQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// Flat
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct FlatPartitionHeader {
    distance_type: u8,
    dim: usize,
    sub_index_len: u64,
    storage_len: u64,
}

impl<S: IvfSubIndex> PartitionEntry<S, FlatQuantizer> {
    /// Serialize this partition entry to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let metadata = self.storage.metadata();
        let distance_type = self.storage.distance_type();

        let sub_index_ipc = write_ipc(&self.index.to_batch()?)?;
        let storage_batches: Vec<_> = self.storage.to_batches()?.collect();
        let storage_ipc = if storage_batches.len() == 1 {
            write_ipc(&storage_batches[0])?
        } else {
            return Err(Error::io(
                "expected exactly one storage batch for Flat storage".to_string(),
            ));
        };

        let header = FlatPartitionHeader {
            distance_type: distance_type_to_u8(distance_type),
            dim: metadata.dim,
            sub_index_len: sub_index_ipc.len() as u64,
            storage_len: storage_ipc.len() as u64,
        };

        let header_json = serde_json::to_vec(&header)?;
        let total_len = 8 + header_json.len() + sub_index_ipc.len() + storage_ipc.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(header_json.len() as u64).to_le_bytes());
        out.extend_from_slice(&header_json);
        out.extend_from_slice(&sub_index_ipc);
        out.extend_from_slice(&storage_ipc);
        Ok(out)
    }

    /// Deserialize a partition entry from bytes, zero-copy for Arrow data.
    pub fn deserialize(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::io("partition data too small".to_string()));
        }
        let header_len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let header_end = 8 + header_len;
        if data.len() < header_end {
            return Err(Error::io("partition data truncated in header".to_string()));
        }

        let header: FlatPartitionHeader = serde_json::from_slice(&data[8..header_end])?;
        let distance_type = u8_to_distance_type(header.distance_type)?;

        let sub_index_start = header_end;
        let sub_index_end = sub_index_start + header.sub_index_len as usize;
        let storage_start = sub_index_end;
        let storage_end = storage_start + header.storage_len as usize;

        if data.len() < storage_end {
            return Err(Error::io(
                "partition data truncated in IPC sections".to_string(),
            ));
        }

        let buffer = Buffer::from(data);
        let sub_index_buf =
            buffer.slice_with_length(sub_index_start, header.sub_index_len as usize);
        let storage_buf = buffer.slice_with_length(storage_start, header.storage_len as usize);

        let sub_index_batch = read_ipc_zero_copy(sub_index_buf)?;
        let storage_batch = read_ipc_zero_copy(storage_buf)?;

        let index = S::load(sub_index_batch)?;
        let metadata = FlatMetadata { dim: header.dim };
        let storage = <FlatQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// SQ
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct SqPartitionHeader {
    distance_type: u8,
    num_bits: u16,
    dim: usize,
    bounds_start: f64,
    bounds_end: f64,
    sub_index_len: u64,
    storage_len: u64,
}

impl<S: IvfSubIndex> PartitionEntry<S, ScalarQuantizer> {
    /// Serialize this partition entry to bytes.
    ///
    /// Multiple SQ storage chunks are concatenated into a single IPC section.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let metadata = self.storage.metadata();
        let distance_type = self.storage.distance_type();

        let sub_index_ipc = write_ipc(&self.index.to_batch()?)?;

        // Write all SQ chunks as multiple record batches in one IPC file, avoiding copies.
        let batches: Vec<_> = self.storage.to_batches()?.collect();
        if batches.is_empty() {
            return Err(Error::io("SQ storage has no batches".to_string()));
        }
        let storage_ipc = write_ipc_batches(&batches)?;

        let header = SqPartitionHeader {
            distance_type: distance_type_to_u8(distance_type),
            num_bits: metadata.num_bits,
            dim: metadata.dim,
            bounds_start: metadata.bounds.start,
            bounds_end: metadata.bounds.end,
            sub_index_len: sub_index_ipc.len() as u64,
            storage_len: storage_ipc.len() as u64,
        };

        let header_json = serde_json::to_vec(&header)?;
        let total_len = 8 + header_json.len() + sub_index_ipc.len() + storage_ipc.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(header_json.len() as u64).to_le_bytes());
        out.extend_from_slice(&header_json);
        out.extend_from_slice(&sub_index_ipc);
        out.extend_from_slice(&storage_ipc);
        Ok(out)
    }

    /// Deserialize a partition entry from bytes, zero-copy for Arrow data.
    pub fn deserialize(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::io("partition data too small".to_string()));
        }
        let header_len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let header_end = 8 + header_len;
        if data.len() < header_end {
            return Err(Error::io("partition data truncated in header".to_string()));
        }

        let header: SqPartitionHeader = serde_json::from_slice(&data[8..header_end])?;
        let distance_type = u8_to_distance_type(header.distance_type)?;

        let sub_index_start = header_end;
        let sub_index_end = sub_index_start + header.sub_index_len as usize;
        let storage_start = sub_index_end;
        let storage_end = storage_start + header.storage_len as usize;

        if data.len() < storage_end {
            return Err(Error::io(
                "partition data truncated in IPC sections".to_string(),
            ));
        }

        let buffer = Buffer::from(data);
        let sub_index_buf =
            buffer.slice_with_length(sub_index_start, header.sub_index_len as usize);
        let storage_buf = buffer.slice_with_length(storage_start, header.storage_len as usize);

        let sub_index_batch = read_ipc_zero_copy(sub_index_buf)?;
        let storage_batches = read_ipc_all_zero_copy(storage_buf)?;

        let index = S::load(sub_index_batch)?;
        let metadata = ScalarQuantizationMetadata {
            dim: header.dim,
            num_bits: header.num_bits,
            bounds: header.bounds_start..header.bounds_end,
        };
        let storage = <ScalarQuantizer as Quantization>::Storage::try_new(
            metadata.num_bits,
            distance_type,
            metadata.bounds,
            storage_batches,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// RabitQ
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct RabitPartitionHeader {
    distance_type: u8,
    num_bits: u8,
    code_dim: u32,
    /// 0 = Matrix, 1 = Fast
    rotation_type: u8,
    /// Fast rotation signs (only set when rotation_type == Fast).
    fast_rotation_signs: Option<Vec<u8>>,
    sub_index_len: u64,
    /// Length of the rotation matrix IPC section; 0 when rotation_type == Fast.
    rotate_mat_len: u64,
    storage_len: u64,
}

impl<S: IvfSubIndex> PartitionEntry<S, RabitQuantizer> {
    /// Serialize this partition entry to bytes.
    ///
    /// For Matrix rotation the rotation matrix is stored as an Arrow IPC section.
    /// For Fast rotation the signs are stored compactly in the JSON header.
    ///
    /// The storage batch is stored with already-packed codes so deserialization
    /// can skip re-packing.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let metadata = self.storage.metadata();
        let distance_type = self.storage.distance_type();

        let sub_index_ipc = write_ipc(&self.index.to_batch()?)?;

        let rotate_mat_ipc = match metadata.rotation_type {
            RQRotationType::Matrix => {
                let mat = metadata.rotate_mat.as_ref().ok_or_else(|| {
                    Error::io(
                        "RabitQ Matrix metadata missing rotate_mat during serialization"
                            .to_string(),
                    )
                })?;
                write_ipc(&fsl_to_batch(mat, "rotate_mat")?)?
            }
            RQRotationType::Fast => Vec::new(),
        };

        let storage_batches: Vec<_> = self.storage.to_batches()?.collect();
        let storage_ipc = if storage_batches.len() == 1 {
            write_ipc(&storage_batches[0])?
        } else {
            return Err(Error::io(
                "expected exactly one storage batch for RabitQ storage".to_string(),
            ));
        };

        let header = RabitPartitionHeader {
            distance_type: distance_type_to_u8(distance_type),
            num_bits: metadata.num_bits,
            code_dim: metadata.code_dim,
            rotation_type: rotation_type_to_u8(metadata.rotation_type),
            fast_rotation_signs: metadata.fast_rotation_signs.clone(),
            sub_index_len: sub_index_ipc.len() as u64,
            rotate_mat_len: rotate_mat_ipc.len() as u64,
            storage_len: storage_ipc.len() as u64,
        };

        let header_json = serde_json::to_vec(&header)?;
        let total_len =
            8 + header_json.len() + sub_index_ipc.len() + rotate_mat_ipc.len() + storage_ipc.len();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(header_json.len() as u64).to_le_bytes());
        out.extend_from_slice(&header_json);
        out.extend_from_slice(&sub_index_ipc);
        out.extend_from_slice(&rotate_mat_ipc);
        out.extend_from_slice(&storage_ipc);
        Ok(out)
    }

    /// Deserialize a partition entry from bytes, zero-copy for Arrow data.
    pub fn deserialize(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::io("partition data too small".to_string()));
        }
        let header_len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let header_end = 8 + header_len;
        if data.len() < header_end {
            return Err(Error::io("partition data truncated in header".to_string()));
        }

        let header: RabitPartitionHeader = serde_json::from_slice(&data[8..header_end])?;
        let distance_type = u8_to_distance_type(header.distance_type)?;
        let rotation_type = u8_to_rotation_type(header.rotation_type)?;

        let sub_index_start = header_end;
        let sub_index_end = sub_index_start + header.sub_index_len as usize;
        let rotate_mat_start = sub_index_end;
        let rotate_mat_end = rotate_mat_start + header.rotate_mat_len as usize;
        let storage_start = rotate_mat_end;
        let storage_end = storage_start + header.storage_len as usize;

        if data.len() < storage_end {
            return Err(Error::io(
                "partition data truncated in IPC sections".to_string(),
            ));
        }

        let buffer = Buffer::from(data);
        let sub_index_buf =
            buffer.slice_with_length(sub_index_start, header.sub_index_len as usize);
        let storage_buf = buffer.slice_with_length(storage_start, header.storage_len as usize);

        let sub_index_batch = read_ipc_zero_copy(sub_index_buf)?;
        let storage_batch = read_ipc_zero_copy(storage_buf)?;

        let rotate_mat = if header.rotate_mat_len > 0 {
            let rotate_mat_buf =
                buffer.slice_with_length(rotate_mat_start, header.rotate_mat_len as usize);
            let mat_batch = read_ipc_zero_copy(rotate_mat_buf)?;
            Some(batch_to_fsl(&mat_batch)?)
        } else {
            None
        };

        let index = S::load(sub_index_batch)?;
        let metadata = RabitQuantizationMetadata {
            rotate_mat,
            rotate_mat_position: None,
            fast_rotation_signs: header.fast_rotation_signs,
            rotation_type,
            code_dim: header.code_dim,
            num_bits: header.num_bits,
            // The storage batch already has packed codes; skip re-packing.
            packed: true,
        };
        let storage = <RabitQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        Float32Array, UInt8Array, UInt64Array,
        types::{Float32Type, UInt8Type},
    };
    use arrow_schema::{DataType, Field, Schema};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::bq::storage::RABIT_CODE_COLUMN;
    use lance_index::vector::bq::transform::{ADD_FACTORS_COLUMN, SCALE_FACTORS_COLUMN};
    use lance_index::vector::bq::{RQRotationType, builder::RabitQuantizer};
    use lance_index::vector::flat::index::FlatIndex;
    use lance_index::vector::flat::storage::FlatFloatStorage;
    use lance_index::vector::sq::storage::ScalarQuantizationStorage;

    // ----- PQ helpers -------------------------------------------------------

    fn make_test_codebook(dim: usize, num_sub_vectors: usize) -> FixedSizeListArray {
        let sub_dim = dim / num_sub_vectors;
        let num_centroids = 256;
        let total_values = num_sub_vectors * num_centroids * sub_dim;
        let values: Vec<f32> = (0..total_values).map(|i| i as f32 * 0.01).collect();
        let values_array = Float32Array::from(values);
        FixedSizeListArray::try_new_from_values(values_array, sub_dim as i32).unwrap()
    }

    fn make_test_pq_storage(
        num_rows: usize,
        dim: usize,
        num_sub_vectors: usize,
    ) -> <ProductQuantizer as Quantization>::Storage {
        let codebook = make_test_codebook(dim, num_sub_vectors);
        let row_ids = UInt64Array::from((0..num_rows as u64).collect::<Vec<_>>());
        let pq_codes_flat: Vec<u8> = (0..num_rows * num_sub_vectors)
            .map(|i| (i % 256) as u8)
            .collect();
        let pq_codes = UInt8Array::from(pq_codes_flat);
        let pq_codes_fsl =
            FixedSizeListArray::try_new_from_values(pq_codes, num_sub_vectors as i32).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(lance_core::ROW_ID, DataType::UInt64, false),
            Field::new(
                lance_index::vector::PQ_CODE_COLUMN,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::UInt8, true)),
                    num_sub_vectors as i32,
                ),
                false,
            ),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(pq_codes_fsl)]).unwrap();

        <ProductQuantizer as Quantization>::Storage::new(
            codebook,
            batch,
            8,
            num_sub_vectors,
            dim,
            DistanceType::L2,
            false,
            None,
        )
        .unwrap()
    }

    // ----- PQ tests ---------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_pq() {
        let dim = 128;
        let num_sub_vectors = 16;
        let num_rows = 100;

        let storage = make_test_pq_storage(num_rows, dim, num_sub_vectors);
        let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let serialized = entry.serialize().unwrap();
        let deserialized =
            PartitionEntry::<FlatIndex, ProductQuantizer>::deserialize(serialized.into()).unwrap();

        assert_eq!(entry.storage, deserialized.storage);
    }

    #[test]
    fn test_roundtrip_preserves_distance_type() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let dim = 32;
            let num_sub_vectors = 4;
            let codebook = make_test_codebook(dim, num_sub_vectors);
            let row_ids = UInt64Array::from(vec![0u64, 1, 2]);
            let pq_codes = UInt8Array::from(vec![0u8; 3 * num_sub_vectors]);
            let pq_codes_fsl =
                FixedSizeListArray::try_new_from_values(pq_codes, num_sub_vectors as i32).unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new(lance_core::ROW_ID, DataType::UInt64, false),
                Field::new(
                    lance_index::vector::PQ_CODE_COLUMN,
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::UInt8, true)),
                        num_sub_vectors as i32,
                    ),
                    false,
                ),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(pq_codes_fsl)])
                    .unwrap();

            let storage = <ProductQuantizer as Quantization>::Storage::new(
                codebook,
                batch,
                8,
                num_sub_vectors,
                dim,
                dt,
                false,
                None,
            )
            .unwrap();

            let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
                index: FlatIndex::default(),
                storage,
            };

            let bytes = entry.serialize().unwrap();
            let restored =
                PartitionEntry::<FlatIndex, ProductQuantizer>::deserialize(bytes.into()).unwrap();
            assert_eq!(
                restored.storage.distance_type(),
                entry.storage.distance_type()
            );
        }
    }

    #[test]
    fn test_empty_partition() {
        let dim = 16;
        let num_sub_vectors = 2;
        let storage = make_test_pq_storage(0, dim, num_sub_vectors);
        let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let serialized = entry.serialize().unwrap();
        let deserialized =
            PartitionEntry::<FlatIndex, ProductQuantizer>::deserialize(serialized.into()).unwrap();
        assert_eq!(entry.storage, deserialized.storage);
    }

    #[test]
    fn test_truncated_data_errors() {
        assert!(
            PartitionEntry::<FlatIndex, ProductQuantizer>::deserialize(Bytes::from_static(
                b"short"
            ))
            .is_err()
        );
    }

    // ----- Flat helpers -----------------------------------------------------

    fn make_flat_storage(num_rows: usize, dim: usize) -> FlatFloatStorage {
        let values: Vec<f32> = (0..num_rows * dim).map(|i| i as f32 * 0.01).collect();
        let values_array = Float32Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_array, dim as i32).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    // ----- Flat tests -------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_flat() {
        let storage = make_flat_storage(50, 64);
        let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = entry.serialize().unwrap();
        let restored =
            PartitionEntry::<FlatIndex, FlatQuantizer>::deserialize(bytes.into()).unwrap();

        assert_eq!(
            restored.storage.metadata().dim,
            entry.storage.metadata().dim
        );
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());
        let orig_batch = entry.storage.to_batches().unwrap().next().unwrap();
        let rest_batch = restored.storage.to_batches().unwrap().next().unwrap();
        assert_eq!(orig_batch, rest_batch);
    }

    #[test]
    fn test_flat_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let values = Float32Array::from(vec![1.0f32; 32]);
            let vectors = FixedSizeListArray::try_new_from_values(values, 32).unwrap();
            let storage = FlatFloatStorage::new(vectors, dt);
            let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = entry.serialize().unwrap();
            let restored =
                PartitionEntry::<FlatIndex, FlatQuantizer>::deserialize(bytes.into()).unwrap();
            assert_eq!(restored.storage.distance_type(), dt);
        }
    }

    // ----- SQ helpers -------------------------------------------------------

    fn make_sq_storage(
        num_rows: usize,
        dim: usize,
        distance_type: DistanceType,
    ) -> ScalarQuantizationStorage {
        let row_ids = UInt64Array::from_iter_values(0..num_rows as u64);
        let sq_codes_flat: Vec<u8> = (0..num_rows * dim).map(|i| (i % 256) as u8).collect();
        let sq_codes = UInt8Array::from(sq_codes_flat);
        let sq_codes_fsl = FixedSizeListArray::try_new_from_values(sq_codes, dim as i32).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(lance_core::ROW_ID, DataType::UInt64, false),
            Field::new(
                lance_index::vector::SQ_CODE_COLUMN,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::UInt8, true)),
                    dim as i32,
                ),
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(sq_codes_fsl)]).unwrap();

        ScalarQuantizationStorage::try_new(8, distance_type, -1.0..1.0, [batch], None).unwrap()
    }

    // ----- SQ tests ---------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_sq() {
        let storage = make_sq_storage(100, 64, DistanceType::L2);
        let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = entry.serialize().unwrap();
        let restored =
            PartitionEntry::<FlatIndex, ScalarQuantizer>::deserialize(bytes.into()).unwrap();

        let m = entry.storage.metadata();
        let rm = restored.storage.metadata();
        assert_eq!(rm.dim, m.dim);
        assert_eq!(rm.num_bits, m.num_bits);
        assert_eq!(rm.bounds, m.bounds);
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());

        // Verify row IDs are preserved.
        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);
    }

    #[test]
    fn test_sq_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let storage = make_sq_storage(10, 16, dt);
            let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = entry.serialize().unwrap();
            let restored =
                PartitionEntry::<FlatIndex, ScalarQuantizer>::deserialize(bytes.into()).unwrap();
            assert_eq!(restored.storage.distance_type(), dt);
        }
    }

    #[test]
    fn test_sq_multiple_chunks_no_copy() {
        // Build SQ storage with multiple chunks by appending batches separately.
        let dim = 16usize;
        let make_batch = |start: u64, n: usize| {
            let row_ids = UInt64Array::from_iter_values(start..start + n as u64);
            let codes = UInt8Array::from(vec![0u8; n * dim]);
            let fsl = FixedSizeListArray::try_new_from_values(codes, dim as i32).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new(lance_core::ROW_ID, DataType::UInt64, false),
                Field::new(
                    lance_index::vector::SQ_CODE_COLUMN,
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::UInt8, true)),
                        dim as i32,
                    ),
                    false,
                ),
            ]));
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(fsl)]).unwrap()
        };
        // Three chunks with 10 rows each.
        let storage = ScalarQuantizationStorage::try_new(
            8,
            DistanceType::L2,
            -1.0..1.0,
            [make_batch(0, 10), make_batch(10, 10), make_batch(20, 10)],
            None,
        )
        .unwrap();
        assert_eq!(storage.len(), 30);

        let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
            index: FlatIndex::default(),
            storage,
        };
        let bytes = entry.serialize().unwrap();
        let restored =
            PartitionEntry::<FlatIndex, ScalarQuantizer>::deserialize(bytes.into()).unwrap();

        assert_eq!(restored.storage.len(), 30);
        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);
    }

    // ----- RabitQ helpers ---------------------------------------------------

    fn make_rabit_storage_fast(
        num_rows: usize,
        code_dim: usize,
        distance_type: DistanceType,
    ) -> <RabitQuantizer as Quantization>::Storage {
        use lance_arrow::FixedSizeListArrayExt;

        let quantizer = RabitQuantizer::new_with_rotation::<Float32Type>(
            1,
            code_dim as i32,
            RQRotationType::Fast,
        );
        // Generate float vectors and quantize them to binary codes.
        let values: Vec<f32> = (0..num_rows * code_dim)
            .map(|i| (i % 100) as f32 / 100.0 - 0.5)
            .collect();
        let values_arr = Float32Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_arr, code_dim as i32).unwrap();
        let codes = quantizer
            .quantize(&vectors)
            .unwrap()
            .as_fixed_size_list()
            .clone();

        let metadata = quantizer.metadata(None);
        let batch = RecordBatch::try_from_iter(vec![
            (
                lance_core::ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..num_rows as u64))
                    as Arc<dyn arrow_array::Array>,
            ),
            (
                RABIT_CODE_COLUMN,
                Arc::new(codes) as Arc<dyn arrow_array::Array>,
            ),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|i| i as f32 * 0.1),
                )) as Arc<dyn arrow_array::Array>,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|i| i as f32 * 0.01 + 0.5),
                )) as Arc<dyn arrow_array::Array>,
            ),
        ])
        .unwrap();

        <RabitQuantizer as Quantization>::Storage::try_from_batch(
            batch,
            &metadata,
            distance_type,
            None,
        )
        .unwrap()
    }

    // ----- RabitQ tests -----------------------------------------------------

    #[test]
    fn test_roundtrip_flat_rabitq_fast() {
        let num_rows = 50;
        let code_dim = 64;
        let storage = make_rabit_storage_fast(num_rows, code_dim, DistanceType::L2);
        let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = entry.serialize().unwrap();
        let restored =
            PartitionEntry::<FlatIndex, RabitQuantizer>::deserialize(bytes.into()).unwrap();

        let m = entry.storage.metadata();
        let rm = restored.storage.metadata();
        assert_eq!(rm.num_bits, m.num_bits);
        assert_eq!(rm.code_dim, m.code_dim);
        assert_eq!(rm.rotation_type, m.rotation_type);
        assert_eq!(rm.fast_rotation_signs, m.fast_rotation_signs);
        assert!(rm.packed);
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());

        // Verify row IDs are preserved.
        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);

        // Verify codes are preserved.
        let orig_batch = entry.storage.to_batches().unwrap().next().unwrap();
        let rest_batch = restored.storage.to_batches().unwrap().next().unwrap();
        let orig_codes = orig_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        let rest_codes = rest_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        assert_eq!(
            orig_codes.values().as_primitive::<UInt8Type>().values(),
            rest_codes.values().as_primitive::<UInt8Type>().values(),
        );
    }

    #[test]
    fn test_rabitq_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let storage = make_rabit_storage_fast(10, 32, dt);
            let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = entry.serialize().unwrap();
            let restored =
                PartitionEntry::<FlatIndex, RabitQuantizer>::deserialize(bytes.into()).unwrap();
            assert_eq!(restored.storage.distance_type(), dt);
        }
    }
}
