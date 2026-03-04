// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization and deserialization of [`VectorIndexDetails`] proto messages.
//!
//! This module handles:
//! - Populating `VectorIndexDetails` from build params at index creation time
//! - Deriving a human-readable index type string (e.g., "IVF_PQ") from details
//! - Serializing details as JSON for `describe_indices()`
//! - Inferring details from index files on disk (fallback for legacy indices)

use std::sync::Arc;

use lance_file::reader::FileReaderOptions;
use lance_index::pb::index::Implementation;
use lance_index::{INDEX_FILE_NAME, INDEX_METADATA_SCHEMA_KEY, pb};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::traits::Reader;
use lance_io::utils::{CachedFileSize, read_last_block, read_version};
use lance_table::format::IndexMetadata;
use serde_json::json;

use super::{StageParams, VectorIndexParams};
use crate::dataset::Dataset;
use crate::index::open_index_proto;
use crate::{Error, Result};

/// Build a `VectorIndexDetails` proto from build params at index creation time.
pub fn vector_index_details(params: &VectorIndexParams) -> prost_types::Any {
    use lance_table::format::pb::VectorIndexDetails;
    use lance_table::format::pb::vector_index_details::*;

    let metric_type = match params.metric_type {
        lance_linalg::distance::DistanceType::L2 => VectorMetricType::L2,
        lance_linalg::distance::DistanceType::Cosine => VectorMetricType::Cosine,
        lance_linalg::distance::DistanceType::Dot => VectorMetricType::Dot,
        lance_linalg::distance::DistanceType::Hamming => VectorMetricType::Hamming,
    };

    let mut target_partition_size = 0u64;
    let mut hnsw_index_config = None;
    let mut compression = None;

    for stage in &params.stages {
        match stage {
            StageParams::Ivf(ivf) => {
                if let Some(tps) = ivf.target_partition_size {
                    target_partition_size = tps as u64;
                }
            }
            StageParams::Hnsw(hnsw) => {
                hnsw_index_config = Some(hnsw.into());
            }
            StageParams::PQ(pq) => {
                compression = Some(Compression::Pq(pq.into()));
            }
            StageParams::SQ(sq) => {
                compression = Some(Compression::Sq(sq.into()));
            }
            StageParams::RQ(rq) => {
                compression = Some(Compression::Rq(rq.into()));
            }
        }
    }

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size,
        hnsw_index_config,
        compression,
    };
    prost_types::Any::from_msg(&details).unwrap()
}

pub fn vector_index_details_default() -> prost_types::Any {
    let details = lance_table::format::pb::VectorIndexDetails::default();
    prost_types::Any::from_msg(&details).unwrap()
}

/// Returns true if the proto value represents a "truly empty" VectorIndexDetails
/// (i.e., a legacy index that was created before we populated this field).
fn is_empty_vector_details(details: &prost_types::Any) -> bool {
    details.value.is_empty()
}

/// Derive a human-readable index type string from VectorIndexDetails.
pub fn derive_vector_index_type(details: &prost_types::Any) -> String {
    use lance_table::format::pb::VectorIndexDetails;
    use lance_table::format::pb::vector_index_details::Compression;

    if is_empty_vector_details(details) {
        return "Vector".to_string();
    }

    let Ok(d) = details.to_msg::<VectorIndexDetails>() else {
        return "Vector".to_string();
    };
    let has_hnsw = d.hnsw_index_config.is_some();
    match d.compression {
        None | Some(Compression::Flat(_)) => {
            if has_hnsw {
                "IVF_HNSW_FLAT"
            } else {
                "IVF_FLAT"
            }
        }
        Some(Compression::Pq(_)) => {
            if has_hnsw {
                "IVF_HNSW_PQ"
            } else {
                "IVF_PQ"
            }
        }
        Some(Compression::Sq(_)) => {
            if has_hnsw {
                "IVF_HNSW_SQ"
            } else {
                "IVF_SQ"
            }
        }
        Some(Compression::Rq(_)) => "IVF_RQ",
    }
    .to_string()
}

/// Serialize VectorIndexDetails as a JSON string.
pub fn vector_details_as_json(details: &prost_types::Any) -> Result<String> {
    use lance_table::format::pb::VectorIndexDetails;
    use lance_table::format::pb::vector_index_details::*;

    if is_empty_vector_details(details) {
        return Ok("{}".to_string());
    }

    let d = details
        .to_msg::<VectorIndexDetails>()
        .map_err(|e| Error::index(format!("Failed to deserialize VectorIndexDetails: {}", e)))?;

    let metric_type = match VectorMetricType::try_from(d.metric_type) {
        Ok(VectorMetricType::L2) => "L2",
        Ok(VectorMetricType::Cosine) => "COSINE",
        Ok(VectorMetricType::Dot) => "DOT",
        Ok(VectorMetricType::Hamming) => "HAMMING",
        Err(_) => "UNKNOWN",
    };

    let mut obj = serde_json::Map::new();
    obj.insert(
        "metric_type".to_string(),
        serde_json::Value::String(metric_type.to_string()),
    );

    if d.target_partition_size > 0 {
        obj.insert(
            "target_partition_size".to_string(),
            json!(d.target_partition_size),
        );
    }

    if let Some(hnsw) = d.hnsw_index_config {
        obj.insert(
            "hnsw".to_string(),
            json!({
                "max_connections": hnsw.max_connections,
                "construction_ef": hnsw.construction_ef,
            }),
        );
    }

    if let Some(compression) = d.compression {
        let comp_json = match compression {
            Compression::Flat(_) => json!({"type": "flat"}),
            Compression::Pq(pq) => json!({
                "type": "pq",
                "num_bits": pq.num_bits,
                "num_sub_vectors": pq.num_sub_vectors,
            }),
            Compression::Sq(sq) => json!({
                "type": "sq",
                "num_bits": sq.num_bits,
            }),
            Compression::Rq(rq) => json!({
                "type": "rq",
                "num_bits": rq.num_bits,
            }),
        };
        obj.insert("compression".to_string(), comp_json);
    }

    Ok(serde_json::Value::Object(obj).to_string())
}

/// Infer VectorIndexDetails from index files on disk.
/// Used as a fallback for legacy indices where the manifest doesn't have populated details.
pub async fn infer_vector_index_details(
    dataset: &Dataset,
    index: &IndexMetadata,
) -> Result<prost_types::Any> {
    let uuid = index.uuid.to_string();
    let index_dir = dataset.indice_files_dir(index)?;
    let index_file = index_dir.child(uuid.as_str()).child(INDEX_FILE_NAME);
    let reader: Arc<dyn Reader> = dataset.object_store.open(&index_file).await?.into();

    let tailing_bytes = read_last_block(reader.as_ref()).await?;
    let (major_version, minor_version) = read_version(&tailing_bytes)?;

    match (major_version, minor_version) {
        (0, 1) | (0, 0) => {
            // Legacy v0.1: read pb::Index, extract VectorIndex stages
            let proto = open_index_proto(reader.as_ref()).await?;
            convert_legacy_proto_to_details(&proto)
        }
        _ => {
            // v0.2+/v0.3: read lance file schema metadata
            convert_v3_metadata_to_details(dataset, &index_file).await
        }
    }
}

fn convert_legacy_proto_to_details(proto: &pb::Index) -> Result<prost_types::Any> {
    use lance_table::format::pb::VectorIndexDetails;
    use lance_table::format::pb::vector_index_details::*;
    use pb::vector_index_stage::Stage;

    let Some(Implementation::VectorIndex(vector_index)) = &proto.implementation else {
        return Ok(vector_index_details_default());
    };

    let metric_type = match pb::VectorMetricType::try_from(vector_index.metric_type) {
        Ok(pb::VectorMetricType::L2) => VectorMetricType::L2,
        Ok(pb::VectorMetricType::Cosine) => VectorMetricType::Cosine,
        Ok(pb::VectorMetricType::Dot) => VectorMetricType::Dot,
        Ok(pb::VectorMetricType::Hamming) => VectorMetricType::Hamming,
        Err(_) => VectorMetricType::L2,
    };

    let mut compression = None;
    for stage in &vector_index.stages {
        if let Some(Stage::Pq(pq)) = &stage.stage {
            compression = Some(Compression::Pq(ProductQuantization {
                num_bits: pq.num_bits,
                num_sub_vectors: pq.num_sub_vectors,
            }));
        }
    }

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size: 0,
        hnsw_index_config: None,
        compression,
    };
    Ok(prost_types::Any::from_msg(&details).unwrap())
}

async fn convert_v3_metadata_to_details(
    dataset: &Dataset,
    index_file: &object_store::path::Path,
) -> Result<prost_types::Any> {
    use lance_index::vector::bq::storage::RABIT_METADATA_KEY;
    use lance_index::vector::hnsw::HnswMetadata;
    use lance_index::vector::ivf::storage::IVF_PARTITION_KEY;
    use lance_index::vector::pq::storage::{PQ_METADATA_KEY, ProductQuantizationMetadata};
    use lance_index::vector::sq::storage::{SQ_METADATA_KEY, ScalarQuantizationMetadata};
    use lance_table::format::pb::vector_index_details::*;
    use lance_table::format::pb::{HnswIndexDetails, VectorIndexDetails};

    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );
    let file = scheduler
        .open_file(index_file, &CachedFileSize::unknown())
        .await?;
    let reader = lance_file::reader::FileReader::try_open(
        file,
        None,
        Default::default(),
        &dataset.metadata_cache.file_metadata_cache(index_file),
        FileReaderOptions::default(),
    )
    .await?;

    let metadata = &reader.schema().metadata;

    // Get distance_type from index metadata
    let metric_type = if let Some(idx_meta_str) = metadata.get(INDEX_METADATA_SCHEMA_KEY) {
        let idx_meta: lance_index::IndexMetadata = serde_json::from_str(idx_meta_str)?;
        match idx_meta.distance_type.to_uppercase().as_str() {
            "L2" | "EUCLIDEAN" => VectorMetricType::L2,
            "COSINE" => VectorMetricType::Cosine,
            "DOT" => VectorMetricType::Dot,
            "HAMMING" => VectorMetricType::Hamming,
            _ => VectorMetricType::L2,
        }
    } else {
        VectorMetricType::L2
    };

    // Check for compression
    let compression = if let Some(pq_str) = metadata.get(PQ_METADATA_KEY) {
        let pq_meta: ProductQuantizationMetadata = serde_json::from_str(pq_str)?;
        Some(Compression::Pq(ProductQuantization {
            num_bits: pq_meta.nbits as u32,
            num_sub_vectors: pq_meta.num_sub_vectors as u32,
        }))
    } else if let Some(sq_str) = metadata.get(SQ_METADATA_KEY) {
        let sq_meta: ScalarQuantizationMetadata = serde_json::from_str(sq_str)?;
        Some(Compression::Sq(ScalarQuantization {
            num_bits: sq_meta.num_bits as u32,
        }))
    } else if let Some(rq_str) = metadata.get(RABIT_METADATA_KEY) {
        let rq_meta: lance_index::vector::bq::storage::RabitQuantizationMetadata =
            serde_json::from_str(rq_str)?;
        let rotation_type = match rq_meta.rotation_type {
            lance_index::vector::bq::RQRotationType::Fast => rabit_quantization::RotationType::Fast,
            lance_index::vector::bq::RQRotationType::Matrix => {
                rabit_quantization::RotationType::Matrix
            }
        };
        Some(Compression::Rq(RabitQuantization {
            num_bits: rq_meta.num_bits as u32,
            rotation_type: rotation_type.into(),
        }))
    } else {
        None
    };

    // Check for HNSW
    let hnsw_index_config = if let Some(partition_str) = metadata.get(IVF_PARTITION_KEY) {
        let partitions: Vec<HnswMetadata> = serde_json::from_str(partition_str)?;
        partitions.first().map(|hnsw| HnswIndexDetails {
            max_connections: hnsw.params.m as u32,
            construction_ef: hnsw.params.ef_construction as u32,
        })
    } else {
        None
    };

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size: 0,
        hnsw_index_config,
        compression,
    };
    Ok(prost_types::Any::from_msg(&details).unwrap())
}
