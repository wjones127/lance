# Indices in Lance

Lance supports three main categories of indices to accelerate data access: scalar
indices, vector indices, and system indices.

**Scalar indices** are traditional indices that speed up queries on scalar data types, such as 
integers and strings. Examples include [B-trees](scalar/btree.md) and
[full-text search indices](scalar/full_text.md). Typically, scalar indices receive a
query predicate, such as equality or range conditions, and output a set of row addresses that
satisfy the predicate.

<figure markdown="span">
  ![](./scalar_index.drawio.svg)
</figure>

**[Vector indices](./vector/index.md)** are specialized for approximate nearest neighbor (ANN) search on high-dimensional
vector data, such as embeddings from machine learning models. Examples includes IVF (Inverted File)
indices and HNSW (Hierarchical Navigable Small World) indices. These are separate from scalar indices
because they use meaningfully different query patterns. Instead of sargable predicates, vector indices
receive a query vector and return the nearest neighbor row addresses based on some distance metric,
such as Euclidean distance or cosine similarity. They return row addresses and the corresponding distances.

**System indices** are auxiliary indices that help accelerate internal system operations. They are
different from user-facing scalar and vector indices, as they are not directly used in user queries.

## Design

Lance indices are designed with the following design choices in mind:

1. **Indexes are loaded on demand**: A dataset and be loaded and read without loading any indices.
   Indices are only loaded when a query can benefit from them.
   This design minimizes memory usage and speeds up dataset opening time.
2. **Indexes can be loaded progressively**: indexes are designed so that only the necessary parts
   are loaded into memory during query execution. For example, when querying a B-tree index,
   it loads a small page table to figure out which pages of the index to load for the given query,
   and then only loads those pages to perform the indexed search. This amortizes the cost of
   cold index queries, since each query only needs to load a small portion of the index.
3. **Indexes can be coalesced to larger units than fragments.** Indexes are much smaller than
   data files, so it is efficient to coalesce index segments to cover multiple fragments.
   This reduces the number of index files that need to be opened during query execution and
   then number of unique index data structures that need to be queried.
4. **Index files are immutable once written, similar to data files.** They can be modified only
   by creating new files. This means they can be safely cached in memory or on disk without
   worrying about consistency issues.

## Basic Concepts

An index in Lance is defined over a specific column (or multiple columns) of a dataset.
It is identified by its name.

An index is made up of multiple **index segments**. (These are sometimes called "delta indices".)
These segments are identified by their unique UUIDs.

Each index segment covers a disjoint subset of fragments in the dataset. The segments must cover
all rows in the fragments they cover, with one exception: if a fragment has delete markers at the time
of index creation, the index segment is allowed to ignore the deleted rows. The fragments an index
covers are those recorded in the `fragment_bitmap` field minus any fragments in the
`invalidated_fragments` field of the index segment metadata.

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
  fragment 1. Fragment 2 is not covered by the index. Queries using this index will need to query both
  segments and then scan fragment 2 directly. Additionally, when querying the segment covering fragment 1,
  the engine will need to filter out the 10 deleted rows.
* There is another index called "vec_idx", which has a single segment covering all three fragments.
  Because it covers all fragments, queries using this index do not need to scan any fragments directly.
  They do, however, need to filter out the 10 deleted rows from fragment 1.

## Index Storage

The content of each index is stored at `_indices/{UUID}` directory under the dataset directory.
We call this location the **index directory**.
The actual content stored in the index directory depends on the index type. These can be
arbitrary files defined by the index implementation. However, often they are made up of
Lance files containing the index data structures. The allows reuse of the existing Lance
file format code for reading and writing index data.

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


<details>
  <summary>Full protobuf definitions</summary>

There are both part of the `table.proto` file in the Lance source code.

```protobuf
%%% proto.message.IndexSection %%%

%%% proto.message.IndexMetadata %%%
```

</details>

## Handling deleted rows

Since index segments are immutable, they main contain references to rows that have been deleted
or updated. These should be filtered out during query execution.

<figure markdown="span">
  ![](./indices-fragment handling.drawio.svg)
  <figcaption>Representation of index segment covering fragments that have deleted rows,
  completely deleted fragments, and updated fragments.
  </figcaption>
</figure>

There are three situations to consider:

1. **A fragment has some deleted rows.** A few of the rows in the fragment have been marked
   as deleted, but some of the rows are still present. The row addresses from the deletion
   file should be used to filter out results from the index.
2. **A fragment has been completely deleted.** This can be detected by checking if a
   fragment ID present in the fragment bitmap is missing from the dataset. (This is why
   it is important for index segments to not mutate the fragment bitmap after creation.)
   Any row addresses from this fragment should be filtered out.
3. **A fragment has had the indexed column updated in place.** This can be detected by checking
   if a fragment ID present in the fragment bitmap is also present in the `invalidated_fragments`
   field of the index segment metadata. Any row addresses from this fragment should be filtered out.
   The difference between this case and the second case is that the fragment still exists, and thus
   will need to be handled outside of this index segment. This might mean that the fragment exists
   in another index segment, or it might need to be scanned directly.

## Merging index segments

Often, it's possible to merge multiple index segments into a single segment without having
to rebuild the index from the data files. However, care should be taken to handle deletions
and invalidated fragments correctly:

1. Deleted fragments should either (a) have their row addresses removed from the merged index,
   or (b) be included in the `fragment_bitmap` of the resulting index. Failing to do either
   could lead to incorrect query results.
2. Updated fragments should either (a) have their row addresses removed from the merged index,
   or (b) be included in the `invalidated_fragments` field of the resulting index.
   Failing to do either could lead to incorrect query results.

## Compaction and remapping

When fragments are compacted, the row addresses of the rows in the fragments change.
This means that any index segments referencing those fragments will no longer point
to existing row addresses. There are three ways to handle this:

<figure markdown="span">
![](./indices-compaction.drawio.svg)
</figure>

1. Do nothing and let the index segment not cover those fragments anymore. This approach is
   simple and valid, but it means compaction can immediately make an index out-of-date. This
   is the worst options for query performance.

2. Immediately rewrite the index segments with the row addresses remapped. This approach
   ensures the index is kept up-to-date, but it incurs singificant write ampliciation
   during compaction.

3. Create a [Fragment Reuse Index](system/frag_reuse.md) that maps old row addresses to new
   row addresses. This allows readers to remap the row addresses in memory upon reading
   the index segments. This approach adds some IO and computation overhead during query
   execution, but avoids write amplification during compaction.

## Stable Row ID for Index

Using a stable row ID to replace the row address for an index is a work in progress.
The main benefit is that remap is not needed, and an update only needs to invalidate the index if related column data has changed.
The tradeoff is that it requires an additional index search to translate a stable row ID to the physical row address.
We are still working on evaluating the performance impact of this change before making it more widely used.
