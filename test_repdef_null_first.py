#!/usr/bin/env python3
"""
Test null struct element specifically at offset 0 (first element in list).

This is what happens in the Rust test:
// 6: [null, {tag: "g"}] — null struct element at position 0
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])

print("=" * 70)
print("Testing null struct element at offset 0 in list")
print("=" * 70)

# Create a struct array where first element is null
tag_array = pa.array(["null_struct", "g", "h", "i"])
struct_array = pa.StructArray.from_arrays(
    [tag_array],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    # First struct is null
    mask=pa.array([False, True, True, True])
)

# Create list: [0, 2], [2, 4]
# First list: indices 0-1 (first struct is null)
# Second list: indices 2-3
offsets = pa.array([0, 2, 4], type=pa.int32())

list_array = pa.ListArray.from_arrays(offsets, struct_array)

batch = pa.record_batch(
    [pa.array([0, 1]), list_array],
    names=["id", "value"]
)

print("Struct array with null at index 0:")
print(f"  Structs: {struct_array}")
print(f"  List offsets: {offsets.to_pylist()}")
print(f"  First list: [null, {{'tag': 'g'}}]")
print()

try:
    with tempfile.TemporaryDirectory(prefix="lance-repdef-first-") as tmp:
        print("Writing...")
        ds = lance.write_dataset(batch, Path(tmp) / "ds")
        print("✅ Write successful")

        print("Reading...")
        result = ds.to_table()
        print("✅ Read successful")

except Exception as e:
    print(f"❌ FAILED: {e}")
    if "repdef" in str(e).lower() or "assertion" in str(e).lower():
        print("🎯 FOUND A PANIC RELATED TO REPDEF/ASSERTION!")
    import traceback
    traceback.print_exc()

# Also try with multiple nulls at the start
print("\n" + "=" * 70)
print("Testing multiple null struct elements at start of list")
print("=" * 70)

tag_array2 = pa.array(["null1", "null2", "valid", "valid"])
struct_array2 = pa.StructArray.from_arrays(
    [tag_array2],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    # First two structs are null
    mask=pa.array([False, False, True, True])
)

offsets2 = pa.array([0, 4], type=pa.int32())
list_array2 = pa.ListArray.from_arrays(offsets2, struct_array2)

batch2 = pa.record_batch(
    [pa.array([0]), list_array2],
    names=["id", "value"]
)

print("List with two null struct elements at start:")
print(f"  [null, null, {{'tag': 'valid'}}, {{'tag': 'valid'}}]")
print()

try:
    with tempfile.TemporaryDirectory(prefix="lance-repdef-multi-") as tmp:
        print("Writing...")
        ds = lance.write_dataset(batch2, Path(tmp) / "ds")
        print("✅ Write successful")

        print("Reading...")
        result = ds.to_table()
        print("✅ Read successful")

except Exception as e:
    print(f"❌ FAILED: {e}")
    if "repdef" in str(e).lower() or "assertion" in str(e).lower():
        print("🎯 FOUND A PANIC RELATED TO REPDEF/ASSERTION!")
    import traceback
    traceback.print_exc()
