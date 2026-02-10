#!/usr/bin/env python3
"""
Verify that null struct elements are being dropped on round-trip.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])

# Create struct array with explicit nulls
tag_array = pa.array(["valid", "null_struct", "valid", "valid"])
struct_array = pa.StructArray.from_arrays(
    [tag_array],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    mask=pa.array([True, False, True, True]),  # 2nd struct is null
)

# Create list: [0, 1, 2, 3]
offsets = pa.array([0, 4], type=pa.int32())
list_array = pa.ListArray.from_arrays(offsets, struct_array)

batch = pa.record_batch([pa.array([0]), list_array], names=["id", "value"])

print("ORIGINAL DATA:")
print("=" * 70)
print(batch)
print()

# Get original struct array validity
original_value = batch.column("value")
original_list = original_value[0].as_py()
print(f"Original list (Python): {original_value[0].as_py()}")
print(f"Original struct array validity: {original_value.values.buffers()[0]}")
print()

with tempfile.TemporaryDirectory(prefix="lance-null-loss-") as tmp:
    ds = lance.write_dataset(batch, Path(tmp) / "ds")
    result = ds.to_table()

    print("AFTER ROUND-TRIP:")
    print("=" * 70)
    print(result)
    print()

    result_value = result.column("value")
    result_list = result_value[0].as_py()
    print(f"Result list (Python): {result_value[0].as_py()}")
    print(f"Result struct array validity: {result_value.values.buffers()[0]}")
    print()

    # Check if they're equal
    print("COMPARISON:")
    print("=" * 70)
    if batch.equals(result.to_batches()[0]):
        print("✅ Data matches exactly!")
    else:
        print("❌ DATA MISMATCH!")

        # Check each struct element
        print("\nDetailed comparison:")
        original_structs = original_value[0].as_py()
        result_structs = result_value[0].as_py()

        for i, (orig, res) in enumerate(zip(original_structs, result_structs)):
            if orig is None and res is not None:
                print(f"  Index {i}: NULL STRUCT LOST! Was None, now {res}")
            elif orig != res:
                print(f"  Index {i}: Changed from {orig} to {res}")

print("\nCONCLUSION:")
print("=" * 70)
if (
    batch.column("value")[0].values.buffers()[0]
    != result.column("value")[0].values.buffers()[0]
):
    print("⚠️  NULL STRUCT ELEMENTS ARE BEING LOST ON ROUND-TRIP")
    print("This is different from the encoder panicking - it's silently dropping nulls")
