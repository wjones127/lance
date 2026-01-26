// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Integration tests for vector index operations.
//!
//! These tests are slow due to the compute-heavy nature of vector index
//! operations, but benefit significantly from running with optimization
//! (opt-level=1 or higher). They are gated behind the `slow_tests` feature
//! and run as part of the `query-integration-tests` CI job.

mod utils;

mod ivf_hnsw;
mod ivf_pq;
mod lifecycle;
mod pq_model;
