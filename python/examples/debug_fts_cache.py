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


def format_bytes(nbytes):
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if nbytes < 1024:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024
    return f"{nbytes:.1f}TB"


def print_step(label, session):
    """Print RSS and cache stats, showing top cache entries by Arc sharing."""
    global _prev_entries

    rss = get_rss_mb()
    idx_stats = session.index_cache_stats()
    meta_stats = session.metadata_cache_stats()

    print(f"\n{'=' * 80}")
    print(f"  {label}")
    print(f"{'=' * 80}")
    print(f"  RSS: {rss:.1f} MB")
    print(f"  Index cache: {idx_stats}")
    print(f"  Metadata cache: {meta_stats}")

    # Get debug info from the session
    reader = session.debug_index_cache()
    try:
        import pyarrow as pa

        # Collect batches manually to handle empty readers
        batches = list(reader)

        if not batches:
            print("\n  Index cache is empty (no entries)")
            return

        table = pa.Table.from_batches(batches)

        if len(table) > 0:
            # Calculate discrepancy and sort
            df = table.to_pandas()
            df["discrepancy"] = df["size_bytes"] - df["incremental_size_bytes"]
            df["sharing_ratio"] = df["incremental_size_bytes"] / (
                df["size_bytes"] + 1e-9
            )  # Avoid division by zero

            # Sort by discrepancy descending
            df_sorted = df.nlargest(20, "discrepancy")

            msg = (
                "  Top 20 entries with biggest size_bytes vs incremental_size_bytes"
                " discrepancy:"
            )
            print(f"\n{msg}")
            print(
                f"  {'Key':<50} {'Type':<20} {'Size (B)':<12} {'Incr (B)':<12} "
                f"{'Discrep':<12} {'Sharing%':<10}"
            )
            print(f"  {'-' * 116}")

            for _, row in df_sorted.iterrows():
                key_short = row["key"][-45:] if len(row["key"]) > 45 else row["key"]
                type_short = (
                    row["type_name"][-18:]
                    if len(row["type_name"]) > 18
                    else row["type_name"]
                )
                size_fmt = format_bytes(int(row["size_bytes"]))
                incr_fmt = format_bytes(int(row["incremental_size_bytes"]))
                discrep_fmt = format_bytes(int(row["discrepancy"]))
                sharing_pct = (1 - row["sharing_ratio"]) * 100

                print(
                    f"  {key_short:<50} {type_short:<20} {size_fmt:>11} {incr_fmt:>11} "
                    f"{discrep_fmt:>11} {sharing_pct:>8.1f}%"
                )

            print(f"\n  Total entries: {len(table)}")
            total_size = format_bytes(int(df["size_bytes"].sum()))
            total_incr = format_bytes(int(df["incremental_size_bytes"].sum()))
            total_discrep = format_bytes(int(df["discrepancy"].sum()))
            print(f"  Total size_bytes: {total_size}")
            print(f"  Total incremental_size_bytes: {total_incr}")
            print(f"  Total discrepancy (shared allocations): {total_discrep}")
    except Exception as e:
        print(f"  Error reading debug info: {e}")


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
