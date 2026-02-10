#!/usr/bin/env python3
"""
Minimal reproduction for list-of-struct scan issue.

Replicates the Rust test: write data with fragmentation, then scan with ordering.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

# Create test data: List<Struct<tag: string>>
list_struct_type = pa.list_(pa.struct([("tag", pa.string())]))

list_array = pa.array(
    [
        [{"tag": "a"}, {"tag": "b"}],  # 0
        [{"tag": "c"}],  # 1
        None,  # 2: null list
        [],  # 3: empty list
        [{"tag": "a"}, {"tag": None}],  # 4: null in struct field
        [{"tag": "d"}, {"tag": "e"}, {"tag": "f"}],  # 5
    ],
    type=list_struct_type,
)

id_array = pa.array(list(range(len(list_array))))

batch = pa.record_batch([id_array, list_array], names=["id", "value"])

print("Original batch:")
print(batch)
print(f"Original num_rows: {batch.num_rows}")
print()

# Test with different file versions
for version_str in [None, "2.1", "2.2"]:
    print(f"\n{'=' * 60}")
    print(f"Testing with file version: {version_str or 'default'}")
    print(f"{'=' * 60}")

    with tempfile.TemporaryDirectory(prefix="lance-list-struct-") as tmp:
        tmp_path = Path(tmp)

        # Write with fragmentation (like the Rust test does with max_rows_per_file=3)
        ds = lance.write_dataset(batch, tmp_path / "ds", mode="overwrite")

        # Add another batch to create multiple fragments
        batch2 = pa.record_batch(
            [
                pa.array([6, 7, 8]),
                pa.array(
                    [
                        [{"tag": "g"}],  # 6
                        None,  # 7: null
                        [{"tag": "h"}],  # 8
                    ],
                    type=list_struct_type,
                ),
            ],
            names=["id", "value"],
        )

        lance.write_dataset(batch2, tmp_path / "ds", mode="append")

        # Re-open
        ds = lance.dataset(tmp_path / "ds")

        # Scan (what the Rust test does)
        print("\nScanning data:")
        try:
            result = ds.to_table()
            print("✅ Scan successful")
            print(f"Result num_rows: {result.num_rows}")
            print(f"Result schema:\n{result.schema}")

            # Convert original to table and compare
            original_table = pa.table(
                [batch.column("id"), batch.column("value")], names=["id", "value"]
            )

            # Add the second batch
            batch2_table = pa.table(
                [batch2.column("id"), batch2.column("value")], names=["id", "value"]
            )

            combined = pa.concat_tables([original_table, batch2_table])

            if result.equals(combined):
                print("✅ Data matches!")
            else:
                print("❌ Data MISMATCH!")
                print(f"\nExpected row count: {combined.num_rows}")
                print(f"Got row count: {result.num_rows}")

                # Try to identify specific differences
                for i in range(min(combined.num_rows, result.num_rows)):
                    orig_id = combined["id"][i].as_py()
                    result_id = result["id"][i].as_py()

                    if orig_id != result_id:
                        print(
                            f"Row {i}: ID mismatch - expected {orig_id}, got {result_id}"
                        )

        except Exception as e:
            print(f"❌ Scan failed: {e}")
            import traceback

            traceback.print_exc()
