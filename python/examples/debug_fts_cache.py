#!/usr/bin/env python3
"""Debug script to inspect FTS index cache sizing.

Creates a Lance dataset with text data, builds an FTS index, and tracks
RSS + cache metrics at each step to help diagnose inflated cache size_bytes.
"""

import resource
import shutil
import tempfile

import lance
import pyarrow as pa
from lance._datagen import rand_batches


def get_rss_mb():
    """Get current RSS in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / (1024 * 1024)  # macOS reports in bytes


_prev_entries: dict[str, int] = {}


def print_step(label, session):
    """Print RSS and cache stats for a step, including diff from previous."""
    global _prev_entries

    rss = get_rss_mb()
    idx_stats = session.index_cache_stats()
    meta_stats = session.metadata_cache_stats()
    entries = session.index_cache_entries()
    curr = dict(entries)

    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    print(f"  RSS: {rss:.1f} MB")
    print(f"  Index cache: {idx_stats}")
    print(f"  Metadata cache: {meta_stats}")
    print(f"  Index cache entries ({len(entries)}):")
    for key, size in entries:
        print(f"    {key}: {size} bytes")

    added = {k: v for k, v in curr.items() if k not in _prev_entries}
    removed = {k: v for k, v in _prev_entries.items() if k not in curr}
    changed = {
        k: (_prev_entries[k], v)
        for k, v in curr.items()
        if k in _prev_entries and v != _prev_entries[k]
    }

    if added or removed or changed:
        print("  Diff from previous step:")
        for key, size in added.items():
            print(f"    + {key}: {size} bytes")
        for key, size in removed.items():
            print(f"    - {key}: {size} bytes")
        for key, (old, new) in changed.items():
            print(f"    ~ {key}: {old} -> {new} bytes")
    elif _prev_entries:
        print("  Diff from previous step: (no changes)")

    _prev_entries = curr


def main():
    tmpdir = tempfile.mkdtemp(prefix="lance_fts_debug_")
    uri = f"{tmpdir}/test_fts.lance"
    print(f"Using temp dir: {tmpdir}")

    try:
        session = lance.Session(index_cache_size_bytes=128 * 1024 * 1024)

        # Step 1: Create table with text data
        schema = pa.schema(
            [
                pa.field(
                    "text",
                    pa.string(),
                    metadata={"lance-datagen:content-type": "sentence"},
                )
            ]
        )
        # ~100 MiB of text data
        data = rand_batches(schema, num_batches=100, batch_size_bytes=1024 * 1024)
        ds = lance.write_dataset(data, uri)
        ds = lance.dataset(uri, session=session)
        print_step("1. After creating dataset", session)

        # Step 2: Create FTS index
        ds.create_scalar_index("text", index_type="FTS")
        ds = lance.dataset(uri, session=session)
        print_step("2. After creating FTS index", session)

        # Step 3: Run FTS queries
        for query in ["document", "topic 5", "searchable text"]:
            ds.to_table(
                full_text_query=query,
                limit=10,
            )
        print_step("3. After FTS queries", session)

        # Step 4: Append more data
        more_texts = [
            f"appended document {i} with new content about subject {i % 5}"
            for i in range(1000, 2000)
        ]
        more_table = pa.table({"id": range(1000, 2000), "text": more_texts})
        lance.write_dataset(more_table, uri, mode="append")
        ds = lance.dataset(uri, session=session)
        print_step("4. After appending data", session)

        # Step 5: Query again (should use index for original data)
        for query in ["document", "appended", "new content"]:
            ds.to_table(
                full_text_query=query,
                limit=10,
            )
        print_step("5. After querying appended data", session)

        # Step 6: Optimize indices
        ds.optimize.optimize_indices()
        ds = lance.dataset(uri, session=session)
        print_step("6. After optimizing indices", session)

        # Step 7: Compact
        ds.optimize.compact_files()
        ds = lance.dataset(uri, session=session)
        print_step("7. After compacting", session)

        # Step 8: Query again after optimization
        for query in ["document", "topic", "subject"]:
            ds.to_table(
                full_text_query=query,
                limit=10,
            )
        print_step("8. After final queries", session)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
