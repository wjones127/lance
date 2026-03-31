// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Zero-copy Arrow IPC stream read/write utilities.
//!
//! Provides helpers for serializing and deserializing [`RecordBatch`]es as
//! self-delimiting Arrow IPC streams using synchronous [`Read`]/[`Write`] I/O.
//!
//! These are designed for embedding IPC streams inside larger binary formats
//! (e.g. a cache entry that contains multiple IPC sections). Each stream is
//! self-delimiting (schema + batches + EOS marker) and can be read back
//! independently.
//!
//! # Zero-copy reads
//!
//! [`read_ipc_stream`] and [`read_ipc_stream_single`] use [`FileDecoder`]
//! rather than `StreamDecoder`, which lets Arrow reuse the per-message
//! [`Buffer`] allocated during reading for the batch's array data. Each
//! message is read into a fresh aligned [`MutableBuffer`]; the body data is
//! not copied again during decoding.

use std::io::{Read, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::ArrowError;

// ---------------------------------------------------------------------------
// Length-prefixed byte utilities
// ---------------------------------------------------------------------------

/// Write `data` prefixed by its length as a little-endian `u64`.
///
/// Paired with [`read_len_prefixed_bytes`].
pub fn write_len_prefixed_bytes(writer: &mut dyn Write, data: &[u8]) -> Result<(), ArrowError> {
    writer
        .write_all(&(data.len() as u64).to_le_bytes())
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    writer
        .write_all(data)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))
}

/// Read a byte slice written by [`write_len_prefixed_bytes`].
///
/// Reads an 8-byte little-endian length then exactly that many bytes.
pub fn read_len_prefixed_bytes(reader: &mut dyn Read) -> Result<Vec<u8>, ArrowError> {
    let mut len_buf = [0u8; 8];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// IPC stream utilities
// ---------------------------------------------------------------------------

// 4-byte continuation marker used by modern Arrow IPC streams.
const IPC_CONTINUATION: [u8; 4] = [0xff; 4];

/// Write `batch` as a single-batch Arrow IPC stream to `writer`.
pub fn write_ipc_stream(batch: &RecordBatch, writer: &mut dyn Write) -> Result<(), ArrowError> {
    let mut sw = StreamWriter::try_new(&mut *writer, batch.schema_ref())?;
    sw.write(batch)?;
    sw.finish()
}

/// Write all batches from `iter` as a single Arrow IPC stream to `writer`.
///
/// `iter` must yield at least one batch; the schema is inferred from the first
/// batch. Returns `ArrowError::InvalidArgumentError` if the iterator is empty.
/// If you need to write an empty stream (schema only, no rows), construct a
/// `StreamWriter` directly.
pub fn write_ipc_stream_batches<I>(iter: I, writer: &mut dyn Write) -> Result<(), ArrowError>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut iter = iter.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ArrowError::InvalidArgumentError("no batches to serialize".into()))?;
    let mut sw = StreamWriter::try_new(&mut *writer, first.schema_ref())?;
    sw.write(&first)?;
    for batch in iter {
        sw.write(&batch)?;
    }
    sw.finish()
}

/// Read one complete Arrow IPC stream message from `reader` into a contiguous
/// [`Buffer`].
///
/// Returns `None` on EOS (size field == 0) or clean EOF. The returned buffer
/// contains the raw message bytes in the same layout as written, suitable for
/// passing to [`FileDecoder`] for zero-copy decoding.
pub fn read_one_ipc_message(reader: &mut dyn Read) -> Result<Option<Buffer>, ArrowError> {
    // Read the first 4 bytes: either a continuation marker or the size directly
    // (legacy IPC format).
    let mut first4 = [0u8; 4];
    match reader.read_exact(&mut first4) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(ArrowError::IoError(e.to_string(), e)),
    }

    let has_continuation = first4 == IPC_CONTINUATION;
    let size_bytes: [u8; 4] = if has_continuation {
        let mut sb = [0u8; 4];
        reader
            .read_exact(&mut sb)
            .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
        sb
    } else {
        first4
    };
    let meta_size = u32::from_le_bytes(size_bytes) as usize;

    if meta_size == 0 {
        return Ok(None); // EOS
    }

    let mut meta = vec![0u8; meta_size];
    reader
        .read_exact(&mut meta)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;

    let msg = root_as_message(&meta)
        .map_err(|e| ArrowError::ParseError(format!("IPC message parse error: {e}")))?;
    let body_len = msg.bodyLength() as usize;

    // Build one contiguous buffer in the same layout as written.
    // Use MutableBuffer (Arrow's 64-byte-aligned allocator) so that the body
    // data is properly aligned for SIMD operations during decoding.
    //   [continuation: 4 (if present)] [size: 4] [metadata: meta_size] [body: body_len]
    let prefix_len = if has_continuation { 8 } else { 4 };
    let total = prefix_len + meta_size + body_len;
    let mut buf = MutableBuffer::from_len_zeroed(total);
    if has_continuation {
        buf[..4].copy_from_slice(&IPC_CONTINUATION);
        buf[4..8].copy_from_slice(&size_bytes);
    } else {
        buf[..4].copy_from_slice(&size_bytes);
    }
    buf[prefix_len..prefix_len + meta_size].copy_from_slice(&meta);
    if body_len > 0 {
        reader
            .read_exact(&mut buf[prefix_len + meta_size..])
            .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    }

    Ok(Some(buf.into()))
}

/// Extract the prefix length and metadata size from a raw IPC message buffer.
///
/// Modern IPC streams have an 8-byte prefix `[continuation: 4][size: 4]`.
/// Legacy streams have a 4-byte prefix `[size: 4]`. Returns `(prefix_len, meta_size)`.
pub fn parse_ipc_message_prefix(buf: &Buffer) -> Result<(usize, usize), ArrowError> {
    let has_continuation = buf.len() >= 4 && buf[..4] == [0xff; 4];
    if has_continuation {
        if buf.len() < 8 {
            return Err(ArrowError::ParseError(
                "IPC message buffer too short".into(),
            ));
        }
        let meta_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        Ok((8, meta_size))
    } else {
        if buf.len() < 4 {
            return Err(ArrowError::ParseError(
                "IPC message buffer too short".into(),
            ));
        }
        let meta_size = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        Ok((4, meta_size))
    }
}

/// Read all [`RecordBatch`]es from one Arrow IPC stream at the current reader
/// position, until the EOS marker.
///
/// Zero-copy: each batch's array data is backed by per-message [`Buffer`]s
/// allocated during reading rather than copied into a central buffer.
///
/// Uses [`FileDecoder`] directly (rather than `StreamDecoder`) to avoid a
/// known edge case where `StreamDecoder` does not produce a batch for messages
/// with a zero-length body when the message exactly fills the decode buffer.
pub fn read_ipc_stream(reader: &mut dyn Read) -> Result<Vec<RecordBatch>, ArrowError> {
    let schema_buf = read_one_ipc_message(reader)?.ok_or_else(|| {
        ArrowError::ParseError("IPC stream: expected schema message, got EOS".into())
    })?;

    let (prefix_len, meta_size) = parse_ipc_message_prefix(&schema_buf)?;
    let schema_msg = root_as_message(&schema_buf[prefix_len..prefix_len + meta_size])
        .map_err(|e| ArrowError::ParseError(format!("IPC schema parse error: {e}")))?;
    let schema = Arc::new(fb_to_schema(schema_msg.header_as_schema().ok_or_else(
        || ArrowError::ParseError("IPC stream: first message is not a schema".into()),
    )?));
    let mut decoder = FileDecoder::new(schema, schema_msg.version());

    let mut batches = Vec::new();

    loop {
        let Some(buf) = read_one_ipc_message(reader)? else {
            break;
        };

        let (prefix_len, meta_size) = parse_ipc_message_prefix(&buf)?;
        let msg = root_as_message(&buf[prefix_len..prefix_len + meta_size])
            .map_err(|e| ArrowError::ParseError(format!("IPC message parse error: {e}")))?;
        let body_len = msg.bodyLength() as usize;

        // Block offset = 0 since the buffer starts at the message boundary.
        // metaDataLength = prefix_len + meta_size (prefix + flatbuf + padding).
        let block = arrow_ipc::Block::new(0, (prefix_len + meta_size) as i32, body_len as i64);

        match msg.header_type() {
            arrow_ipc::MessageHeader::RecordBatch => {
                if let Some(batch) = decoder.read_record_batch(&block, &buf)? {
                    batches.push(batch);
                }
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                decoder.read_dictionary(&block, &buf)?;
            }
            _ => break,
        }
    }

    Ok(batches)
}

/// Read exactly one [`RecordBatch`] from one Arrow IPC stream.
pub fn read_ipc_stream_single(reader: &mut dyn Read) -> Result<RecordBatch, ArrowError> {
    let mut batches = read_ipc_stream(reader)?;
    match batches.len() {
        1 => Ok(batches.remove(0)),
        n => Err(ArrowError::ParseError(format!(
            "expected exactly 1 IPC record batch, got {n}"
        ))),
    }
}
