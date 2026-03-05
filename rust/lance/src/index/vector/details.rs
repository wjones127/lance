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
use serde::Serialize;

use super::{StageParams, VectorIndexParams};
use crate::dataset::Dataset;
use crate::index::open_index_proto;
use crate::{Error, Result};

// Private structs for JSON serialization of VectorIndexDetails.
// Changes to field names or structure are backwards-incompatible for users
// parsing the JSON output of describe_indices(). See snapshot tests below.

#[derive(Serialize)]
struct VectorDetailsJson {
    metric_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_partition_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hnsw: Option<HnswDetailsJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<CompressionDetailsJson>,
}

#[derive(Serialize)]
struct HnswDetailsJson {
    max_connections: u32,
    construction_ef: u32,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum CompressionDetailsJson {
    Pq {
        num_bits: u32,
        num_sub_vectors: u32,
    },
    Sq {
        num_bits: u32,
    },
    Rq {
        num_bits: u32,
        rotation_type: &'static str,
    },
}

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

/// Returns true if this is a vector index whose details need to be inferred from disk.
///
/// This covers two legacy cases:
/// - Very old indices (<=0.19.2) where `index_details` is `None` but the indexed
///   field is a vector type
/// - Newer pre-details indices where `index_details` has a VectorIndexDetails
///   type_url but empty value bytes
pub fn needs_vector_details_inference(
    index: &IndexMetadata,
    schema: &lance_core::datatypes::Schema,
) -> bool {
    match &index.index_details {
        Some(d) => d.type_url.ends_with("VectorIndexDetails") && d.value.is_empty(),
        None => index.fields.iter().any(|&field_id| {
            schema
                .field_by_id(field_id)
                .map(|f| matches!(f.data_type(), arrow_schema::DataType::FixedSizeList(_, _)))
                .unwrap_or(false)
        }),
    }
}

/// Infer missing vector index details for all indices that need it.
///
/// Runs inference once per unique index name, concurrently across names.
/// Applies the inferred details back to all matching indices in the slice.
pub async fn infer_missing_vector_details(dataset: &Dataset, indices: &mut [IndexMetadata]) {
    use std::collections::HashMap;

    let schema = dataset.schema();
    let needs_inference: HashMap<&str, &IndexMetadata> = indices
        .iter()
        .filter(|idx| needs_vector_details_inference(idx, schema))
        .map(|idx| (idx.name.as_str(), idx))
        .collect();
    if needs_inference.is_empty() {
        return;
    }
    let inferred: HashMap<String, Arc<prost_types::Any>> =
        futures::future::join_all(needs_inference.into_iter().map(
            |(name, representative)| async move {
                let result = infer_vector_index_details(dataset, representative).await;
                (name.to_string(), result)
            },
        ))
        .await
        .into_iter()
        .filter_map(|(name, result)| match result {
            Ok(details) => Some((name, Arc::new(details))),
            Err(err) => {
                log::warn!("Could not infer vector index details for {}: {}", name, err);
                None
            }
        })
        .collect();
    for index in indices.iter_mut() {
        if let Some(details) = inferred.get(&index.name) {
            index.index_details = Some(details.clone());
        }
    }
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
        None => {
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

    let hnsw = d.hnsw_index_config.map(|h| HnswDetailsJson {
        max_connections: h.max_connections,
        construction_ef: h.construction_ef,
    });

    let compression = d.compression.map(|c| match c {
        Compression::Pq(pq) => CompressionDetailsJson::Pq {
            num_bits: pq.num_bits,
            num_sub_vectors: pq.num_sub_vectors,
        },
        Compression::Sq(sq) => CompressionDetailsJson::Sq {
            num_bits: sq.num_bits,
        },
        Compression::Rq(rq) => {
            let rotation_type = match rabit_quantization::RotationType::try_from(rq.rotation_type) {
                Ok(rabit_quantization::RotationType::Matrix) => "matrix",
                _ => "fast",
            };
            CompressionDetailsJson::Rq {
                num_bits: rq.num_bits,
                rotation_type,
            }
        }
    });

    let json = VectorDetailsJson {
        metric_type,
        target_partition_size: if d.target_partition_size > 0 {
            Some(d.target_partition_size)
        } else {
            None
        },
        hnsw,
        compression,
    };

    serde_json::to_string(&json).map_err(|e| Error::index(format!("Failed to serialize: {}", e)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use lance_table::format::pb::vector_index_details::*;
    use lance_table::format::pb::{HnswIndexDetails, VectorIndexDetails};

    fn make_details(
        metric: VectorMetricType,
        hnsw: Option<HnswIndexDetails>,
        compression: Option<Compression>,
    ) -> prost_types::Any {
        let details = VectorIndexDetails {
            metric_type: metric.into(),
            target_partition_size: 0,
            hnsw_index_config: hnsw,
            compression,
        };
        prost_types::Any::from_msg(&details).unwrap()
    }

    #[test]
    fn test_derive_index_type_without_hnsw() {
        // Note: (None, "IVF_FLAT") is not testable here because a proto with
        // all defaults serializes to empty bytes, which is treated as a legacy index.
        let cases: [(Option<Compression>, &str); 3] = [
            (
                Some(Compression::Pq(ProductQuantization {
                    num_bits: 8,
                    num_sub_vectors: 16,
                })),
                "IVF_PQ",
            ),
            (
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 })),
                "IVF_SQ",
            ),
            (
                Some(Compression::Rq(RabitQuantization {
                    num_bits: 1,
                    rotation_type: 0,
                })),
                "IVF_RQ",
            ),
        ];
        for (compression, expected) in cases {
            let details = make_details(VectorMetricType::L2, None, compression);
            assert_eq!(derive_vector_index_type(&details), expected);
        }
    }

    #[test]
    fn test_derive_index_type_with_hnsw() {
        let hnsw = Some(HnswIndexDetails {
            max_connections: 20,
            construction_ef: 150,
        });
        assert_eq!(
            derive_vector_index_type(&make_details(VectorMetricType::L2, hnsw, None)),
            "IVF_HNSW_FLAT"
        );
        assert_eq!(
            derive_vector_index_type(&make_details(
                VectorMetricType::L2,
                hnsw,
                Some(Compression::Pq(ProductQuantization {
                    num_bits: 8,
                    num_sub_vectors: 16,
                }))
            )),
            "IVF_HNSW_PQ"
        );
        assert_eq!(
            derive_vector_index_type(&make_details(
                VectorMetricType::L2,
                hnsw,
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 }))
            )),
            "IVF_HNSW_SQ"
        );
    }

    #[test]
    fn test_derive_index_type_empty_details() {
        let details = vector_index_details_default();
        assert_eq!(derive_vector_index_type(&details), "Vector");
    }

    // Snapshot tests for JSON serialization. These guard backwards compatibility
    // of the JSON format returned by describe_indices().

    #[test]
    fn test_json_ivf_pq() {
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Pq(ProductQuantization {
                num_bits: 8,
                num_sub_vectors: 16,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","compression":{"type":"pq","num_bits":8,"num_sub_vectors":16}}"#
        );
    }

    #[test]
    fn test_json_ivf_hnsw_sq() {
        let details = make_details(
            VectorMetricType::Cosine,
            Some(HnswIndexDetails {
                max_connections: 30,
                construction_ef: 200,
            }),
            Some(Compression::Sq(ScalarQuantization { num_bits: 4 })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"COSINE","hnsw":{"max_connections":30,"construction_ef":200},"compression":{"type":"sq","num_bits":4}}"#
        );
    }

    #[test]
    fn test_json_ivf_rq_with_rotation() {
        let details = make_details(
            VectorMetricType::Dot,
            None,
            Some(Compression::Rq(RabitQuantization {
                num_bits: 1,
                rotation_type: rabit_quantization::RotationType::Matrix as i32,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"DOT","compression":{"type":"rq","num_bits":1,"rotation_type":"matrix"}}"#
        );
    }

    #[test]
    fn test_json_ivf_rq_fast_rotation() {
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Rq(RabitQuantization {
                num_bits: 1,
                rotation_type: rabit_quantization::RotationType::Fast as i32,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","compression":{"type":"rq","num_bits":1,"rotation_type":"fast"}}"#
        );
    }

    #[test]
    fn test_json_with_target_partition_size() {
        let details = {
            let d = VectorIndexDetails {
                metric_type: VectorMetricType::L2.into(),
                target_partition_size: 5000,
                hnsw_index_config: None,
                compression: None,
            };
            prost_types::Any::from_msg(&d).unwrap()
        };
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","target_partition_size":5000}"#
        );
    }

    #[test]
    fn test_json_empty_details() {
        let details = vector_index_details_default();
        assert_eq!(vector_details_as_json(&details).unwrap(), "{}");
    }
}
