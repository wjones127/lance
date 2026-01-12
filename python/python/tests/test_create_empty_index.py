# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Tests for creating empty indices with train=False."""

import lance
import pyarrow as pa
import pyarrow.compute as pc


def test_create_empty_scalar_index():
    data = pa.table({"id": range(100)})
    dataset = lance.write_dataset(data, "memory://")

    # Passing train=False to create an empty index
    dataset.create_scalar_index("id", "BTREE", train=False)

    # Verify index exists and has correct stats
    indices = dataset.list_indices()
    assert len(indices) == 1
    assert indices[0]["type"] == "BTree"
    stats = dataset.stats.index_stats(indices[0]["name"])
    assert stats["num_indexed_rows"] == 0
    assert stats["num_unindexed_rows"] == dataset.count_rows()


def test_create_empty_vector_index():
    dim = 32
    values = pc.random(100 * dim).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    data = pa.table({"vector": vectors})
    dataset = lance.write_dataset(data, "memory://")

    # Create empty vector index with train=False
    dataset.create_index(
        "vector", "IVF_PQ", num_partitions=10, num_sub_vectors=8, train=False
    )

    # Verify index exists and has correct stats (0 indexed rows since it's empty)
    indices = dataset.list_indices()
    assert len(indices) == 1
    assert indices[0]["type"] == "IVF_PQ"
    stats = dataset.stats.index_stats(indices[0]["name"])
    assert stats["num_indexed_rows"] == 0
    assert stats["num_unindexed_rows"] == dataset.count_rows()

    # Query should still work, using brute force KNN
    query = vectors[0].values.to_numpy()
    results = dataset.to_table(
        nearest={"column": "vector", "q": query, "k": 5, "use_index": True}
    )
    assert len(results) == 5


def test_create_empty_vector_index_ivf_flat():
    """Test empty IVF_FLAT vector index."""
    dim = 32
    values = pc.random(100 * dim).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    data = pa.table({"vector": vectors})
    dataset = lance.write_dataset(data, "memory://")

    dataset.create_index("vector", "IVF_FLAT", num_partitions=10, train=False)

    indices = dataset.list_indices()
    assert len(indices) == 1
    assert indices[0]["type"] == "IVF_FLAT"
    stats = dataset.stats.index_stats(indices[0]["name"])
    assert stats["num_indexed_rows"] == 0


def test_create_empty_vector_index_ivf_hnsw_sq():
    """Test empty IVF_HNSW_SQ vector index."""
    dim = 32
    values = pc.random(100 * dim).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    data = pa.table({"vector": vectors})
    dataset = lance.write_dataset(data, "memory://")

    dataset.create_index("vector", "IVF_HNSW_SQ", num_partitions=1, train=False)

    indices = dataset.list_indices()
    assert len(indices) == 1
    assert indices[0]["type"] == "IVF_HNSW_SQ"
    stats = dataset.stats.index_stats(indices[0]["name"])
    assert stats["num_indexed_rows"] == 0


def test_auto_fallback_to_empty_index():
    """Test that datasets with < 256 vectors automatically create empty indices."""
    dim = 32
    # Create dataset with only 100 vectors (< 256 threshold)
    values = pc.random(100 * dim).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    data = pa.table({"vector": vectors})
    dataset = lance.write_dataset(data, "memory://")

    # Even with train=True, should create empty index since < 256 vectors
    dataset.create_index(
        "vector", "IVF_PQ", num_partitions=10, num_sub_vectors=8, train=True
    )

    # Verify index was created empty
    indices = dataset.list_indices()
    assert len(indices) == 1
    stats = dataset.stats.index_stats(indices[0]["name"])
    assert stats["num_indexed_rows"] == 0
    assert stats["num_unindexed_rows"] == 100

    # Query should still work via brute force
    query = vectors[0].values.to_numpy()
    results = dataset.to_table(
        nearest={"column": "vector", "q": query, "k": 5, "use_index": True}
    )
    assert len(results) == 5


def test_optimize_empty_vector_index(tmp_path):
    """Test that optimize_indices() trains empty indices when more data is added."""
    dim = 32
    uri = str(tmp_path / "test_optimize_empty")

    # Start with < 256 vectors to create empty index
    values = pc.random(100 * dim).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    data = pa.table({"vector": vectors})
    dataset = lance.write_dataset(data, uri)

    # Create empty index
    dataset.create_index(
        "vector", "IVF_PQ", num_partitions=2, num_sub_vectors=4, train=False
    )

    stats = dataset.stats.index_stats("vector_idx")
    assert stats["num_indexed_rows"] == 0

    # Add more data to exceed threshold
    more_values = pc.random(300 * dim).cast(pa.float32())
    more_vectors = pa.FixedSizeListArray.from_arrays(more_values, dim)
    more_data = pa.table({"vector": more_vectors})
    dataset = lance.write_dataset(more_data, uri, mode="append")

    # Optimize should train the index now that we have enough data
    dataset.optimize.optimize_indices()

    # Reload to get updated stats
    dataset = lance.dataset(uri)
    stats = dataset.stats.index_stats("vector_idx")
    # After optimization, the index should be trained and have indexed rows
    assert stats["num_indexed_rows"] > 0
