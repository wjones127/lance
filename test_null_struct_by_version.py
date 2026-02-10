#!/usr/bin/env python3
"""
Test null struct element preservation across different file format versions.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])

# Create struct array with null struct element
tag_array = pa.array(["valid", "null_struct", "valid", "valid"])
struct_array = pa.StructArray.from_arrays(
    [tag_array],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    mask=pa.array([True, False, True, True])  # 2nd struct is null
)

# Create list
offsets = pa.array([0, 4], type=pa.int32())
list_array = pa.ListArray.from_arrays(offsets, struct_array)

batch = pa.record_batch(
    [pa.array([0]), list_array],
    names=["id", "value"]
)

print("ORIGINAL DATA:")
print("=" * 70)
print(batch)
print()

# Test with different file versions
for version_str in [None, "2.0", "2.1", "2.2"]:
    print(f"\n{'=' * 70}")
    print(f"Testing file version: {version_str or 'DEFAULT'}")
    print(f"{'=' * 70}")

    with tempfile.TemporaryDirectory(prefix="lance-version-test-") as tmp:
        try:
            # Write with specific version
            if version_str:
                ds = lance.write_dataset(
                    batch,
                    Path(tmp) / "ds",
                    data_storage_version=version_str
                )
            else:
                ds = lance.write_dataset(batch, Path(tmp) / "ds")

            result = ds.to_table()
            result_batch = result.to_batches()[0]

            # Get the file version that was actually written
            manifest_version = ds.version
            print(f"Dataset version: {manifest_version}")

            # Check original vs result
            original_list = batch.column("value")[0].as_py()
            result_list = result.column("value")[0].as_py()

            print(f"\nOriginal: {original_list}")
            print(f"Result:   {result_list}")

            # Check if null struct element was preserved
            if original_list == result_list:
                print("✅ NULL STRUCT ELEMENT PRESERVED")
            else:
                print("❌ NULL STRUCT ELEMENT LOST")
                # Show which elements changed
                for i, (o, r) in enumerate(zip(original_list, result_list)):
                    if o != r:
                        print(f"   Index {i}: {o} -> {r}")

        except Exception as e:
            print(f"❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
