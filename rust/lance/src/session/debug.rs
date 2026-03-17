// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow_array::{RecordBatchIterator, StringArray, UInt64Array};
use itertools::Itertools;
use lance_core::cache::LanceCache;
use std::sync::{Arc, LazyLock, mpsc};
use std::thread;

static LANCE_CACHE_DEBUG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("type_name", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("incremental_size_bytes", DataType::UInt64, false),
    ]))
});

pub fn debug_index_cache(cache: LanceCache) -> Box<dyn RecordBatchReader + Send> {
    let (tx, rx) = mpsc::sync_channel(8);

    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            let _ = cache
                .debug_entries()
                .await
                .chunks(1024)
                .into_iter()
                .for_each(|chunk| {
                    let chunk = chunk.collect_vec();
                    let keys = StringArray::from_iter_values(
                        chunk.iter().map(|entry| entry.key.0.as_str()),
                    );

                    let type_names =
                        StringArray::from_iter_values(chunk.iter().map(|entry| entry.type_name));
                    let size_bytes = chunk
                        .iter()
                        .map(|entry| entry.size_bytes.try_into().unwrap_or(u64::MAX))
                        .collect::<UInt64Array>();
                    let incremental_size_bytes = chunk
                        .iter()
                        .map(|entry| entry.incremental_size_bytes.try_into().unwrap_or(u64::MAX))
                        .collect::<UInt64Array>();

                    let batch = RecordBatch::try_new(
                        LANCE_CACHE_DEBUG_SCHEMA.clone(),
                        vec![
                            Arc::new(keys),
                            Arc::new(type_names),
                            Arc::new(size_bytes),
                            Arc::new(incremental_size_bytes),
                        ],
                    );
                    let _ = tx.send(batch);
                });
        });
    });

    Box::new(RecordBatchIterator::new(
        rx.into_iter(),
        LANCE_CACHE_DEBUG_SCHEMA.clone(),
    ))
}
