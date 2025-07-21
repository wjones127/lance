#!/bin/bash
cd /home/will/Documents/lance
export RUST_LOG=debug
cargo test --manifest-path rust/lance/Cargo.toml test_create_empty_vector_index --lib