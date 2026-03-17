#!/usr/bin/env python3
"""Debug script to inspect BTREE index cache sizing.

Creates a Lance dataset with UUID data, builds BTREE indices, and tracks
RSS + cache metrics at each step to help diagnose cache size_bytes patterns.
"""

import resource
import shutil
import tempfile
import uuid

import lance
import pyarrow as pa


def get_rss_mb():
    """Get current RSS in MB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / (1024 * 1024)  # macOS reports in bytes


def format_bytes(nbytes):
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if nbytes < 1024:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024
    return f"{nbytes:.1f}TB"


def print_step(label, session):
    """Print RSS and cache stats, showing top cache entries by Arc sharing."""
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
    tmpdir = tempfile.mkdtemp(prefix="lance_btree_debug_")
    uri = f"{tmpdir}/test_btree.lance"
    print(f"Using temp dir: {tmpdir}")

    try:
        session = lance.Session(index_cache_size_bytes=256 * 1024 * 1024)

        # Step 1: Create table with UUID data
        num_rows = 100000
        uuids = [str(uuid.uuid4()) for _ in range(num_rows)]
        data = pa.table(
            {
                "id": pa.array(range(num_rows)),
                "uuid": pa.array(uuids),
                "value": pa.array([float(i) for i in range(num_rows)]),
            }
        )
        ds = lance.write_dataset(data, uri)
        ds = lance.dataset(uri, session=session)
        print_step("1. After creating dataset", session)

        # Step 2: Create BTREE index on UUID
        ds.create_scalar_index("uuid", index_type="Btree")
        ds = lance.dataset(uri, session=session)
        print_step("2. After creating BTREE index on uuid", session)

        # Step 3: Run range queries using the index
        # Scan the full table to load index into cache
        ds.prewarm_index("uuid_idx")
        print_step("3. After scanning with index loaded", session)

        # Step 4: Create BTREE index on ID
        ds.create_scalar_index("id", index_type="Btree")
        ds = lance.dataset(uri, session=session)
        print_step("4. After creating BTREE index on id", session)

        # Step 5: Run additional scans
        ds.prewarm_index("uuid_idx")
        print_step("5. After additional scans", session)

        # Step 6: Append more data with new UUIDs
        more_uuids = [str(uuid.uuid4()) for _ in range(50000)]
        more_data = pa.table(
            {
                "id": pa.array(range(num_rows, num_rows + 50000)),
                "uuid": pa.array(more_uuids),
                "value": pa.array(
                    [float(i) for i in range(num_rows, num_rows + 50000)]
                ),
            }
        )
        ds = lance.write_dataset(more_data, uri, mode="append")
        ds = lance.dataset(uri, session=session)
        print_step("6. After appending more data", session)

        # Step 7: Query the new data
        ds.prewarm_index("uuid_idx")
        print_step("7. After querying appended data", session)

        # Step 8: Compact files
        ds.optimize.compact_files(defer_index_remap=True)
        ds = lance.dataset(uri, session=session)
        print_step("8. After compacting", session)

        # Step 9: Final queries on compacted data
        ds.prewarm_index("uuid_idx")
        print_step("9. After final queries on compacted data", session)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
