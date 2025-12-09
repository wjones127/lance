# Indices in Lance

Lance supports three main categories of indices to accelerate data access: scalar
indices, vector indices, and system indices.

**Scalar indices** are traditional indices that speed up queries on scalar data types, such as 
integers and strings. Examples include [B-trees](scalar/btree.md) and
[full-text search indices](scalar/full_text.md). Typically, scalar indices receive a sargable[^1]
query predicate, such as equality or range conditions, and output a set of row addresses that
satisfy the predicate.

[^1]: Sargable: Search ARGument ABLE. A sargable query is one that can take advantage of an index to speed up execution.

![](./scalar_index.drawio.svg)

**Vector indices** are specialized for approximate nearest neighbor (ANN) search on high-dimensional
vector data, such as embeddings from machine learning models. Examples includes IVF (Inverted File)
indices and HNSW (Hierarchical Navigable Small World) indices. These are separate from scalar indices
because they use meaningfully different query patterns. Instead of sargable predicates, vector indices
receive a query vector and return the nearest neighbor row addresses based on some distance metric,
such as Euclidean distance or cosine similarity. They return row addresses and the corresponding distances.

**System indices** are auxiliary indices that help accelerate internal system operations. They are
different from user-facing scalar and vector indices, as they are not directly used in user queries.

## Design philosophy

Lance indices are designed with the following principles in mind:

1. Indexes are loaded on demand: A dataset and be loaded and read without loading any indices.
   Indices are only loaded when a query can benefit from them.
   This design minimizes memory usage and speeds up dataset opening time.
2. Indexes can be loaded progressively: indexes are designed so that only the necessary parts
   are loaded into memory during query execution. For example, when querying a B-tree index,
   it loads a small page table to figure out which pages of the index to load for the given query,
   and then only loads those pages to perform the indexed search. This amortizes the cost of
   cold index queries.
3. Indexes can be coalesced to larger units than fragments. 
4. Similar to data files, index files are immutable once written. They can be modified only
   by creating new files.

## Basic Concepts

An index in Lance is defined over a specific column (or multiple columns) of a dataset.
It is identified by its name.

An index is made up of multiple **index segments**. (These are sometimes called "delta indices".)
These segments are identified by their unique UUIDs.

Each index segment covers a disjoint subset of fragments in the dataset. The segments must cover
all rows in the fragments they cover, with one exception: if a fragment has delete markers at the time
of index creation, the index segment is allowed to ignore the deleted rows.

Index segments together **do not** need to cover all fragments. This means an index isn't required to 
be fully up-to-date. When this happens, engines can split their queries into indexed and unindexed
subplans and merge the results.

<figure markdown="span">
  ![](./starter-example.drawio.svg)
  <figcaption>Abstract layout of a typical dataset, with three fragments and two indices.
  </figcaption>
</figure>

Consider the example dataset in the figure above:

* The dataset contains three fragments with ids 0, 1, 2. Fragment 1 has 10 deleted rows, indicated
  by the deletion file.
* There is an index called "id_idx", which has two segments: one covering fragments 0 and another covering
  fragment 1. Fragment 2 is not covered by the index.
* There is another index called "vec_idx", which has a single segment covering all three fragments.

## Loading an index

When loading an index:

1. Get the offset to the index section from the `index_section` field in the [manifest](../index.md#manifest).
2. Read the index section from the manifest file. This is a protobuf message of type `IndexSection`, which
   contains a list of `IndexMetadata` messages, each describing an index segment.
3. Read the index files from the `_indices/{UUID}` directory under the dataset directory,
   where `{UUID}` is the UUID of the index segment.

!!! tip "Optimizing manifest loading"

    When the manifest file is small, you can read and cache the index section eagerly. This avoids
    an extra file read when loading indices.

The `IndexMetadata` message contains important information about the index segment:

* `uuid`: the unique identifier of the index segment.
* `fields`: the column(s) the index is built on.
* `fragment_bitmap`: the set of fragment IDs covered by this index segment when it
  was created.
* `invalidated_fragments`: the set of fragment IDs that have been invalidated since
  the index segment was created. These fragments should not be used with this index segment.
* `index_details`: a protobuf `Any` message that contains index-specific details, such as index type,
  parameters, and storage format. This allows different index types to store their own metadata.

!!! note "Index protobuf"

    See the full protobuf definition of index metadata in
    [protos/table.proto](https://github.com/lance-format/lance/blob/main/protos/table.proto).

## Handling deleted rows

Since index segments are immutable, they main contain references to rows that have been deleted
or updated. These should be filtered out during query execution.

There are three situations to consider:

1. **A fragment has some deleted rows.**
2. **A fragment has been completely deleted.**
3. **A fragment has had the indexed column updated in place.**

## Adding to an index

## Merging index segments

## Compaction and remapping

## Column replacements and Invalidation


### Index ID, Name and Delta Indices

Each index has a unique UUID. Multiple indices of different IDs can share the same name.
When this happens, these indices are called **Delta Indices** because they together form a complete index.
Delta indices are typically used when the index is updated incrementally to avoid full rebuild.
The Lance SDK provides functions for users to choose when to create delta indices,
and when to merge them back into a single index.

### Index Coverage and Fragment Bitmap

An index records the fragments it covers using a bitmap of the `uint32` fragment IDs, 
so that during the query planning phase, Lance can generate a split plan to leverage the index for covered fragments,
and perform scan for uncovered fragments and merge the results.

### Index Remap and Row Address

In general, indices describe how to find a row address based on some value of a column.
For example, a B-tree index can be used to find the row address of a specific value in a sorted array.

When compaction happens, because the row address has changed and some delete markers are removed, the index needs to be updated accordingly.
This update is fast because it's a pure mapping operation to delete some values or change the old row address to the new row address.
We call this process **Index Remap**.
For more details, see [Fragment Reuse Index](system/frag_reuse.md)

### Stable Row ID for Index

Using a stable row ID to replace the row address for an index is a work in progress.
The main benefit is that remap is not needed, and an update only needs to invalidate the index if related column data has changed.
The tradeoff is that it requires an additional index search to translate a stable row ID to the physical row address.
We are still working on evaluating the performance impact of this change before making it more widely used.

## Index Storage

The content of each index is stored at `_indices/{UUID}` directory under the dataset directory.
We call this location the **index directory**.
The actual content stored in the index directory depends on the index type.
