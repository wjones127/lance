#!/usr/bin/env python3
"""
Test the specific case: null struct element IN a list.

This is different from:
- Null struct field (struct is valid but a field is null)
- Null list (the entire list is null)

We need: List<Struct> where a struct ELEMENT is null.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])
list_struct_type = pa.list_(struct_type)

print("=" * 70)
print("Creating List<Struct> with NULL STRUCT ELEMENTS")
print("=" * 70)

# Create a struct array with some null structs
# If we have 4 struct elements, let's make the 2nd one null
tag_array = pa.array(["a", "b", "c", "d"])

# Create struct array with validity mask
struct_array = pa.StructArray.from_arrays(
    [tag_array],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    # False = null, True = valid
    mask=pa.array([True, False, True, True]),
)

print("\nStruct array (2nd element is null):")
print(struct_array)
print()

# Now we need to create a list that contains these structs
# One way: use the low-level ListArray constructor
# Structure: List containing indices [0, 1], [2, 3]
# So first list has struct 0 and struct 1 (where 1 is null)
# Second list has struct 2 and struct 3

offsets = pa.array([0, 2, 4], type=pa.int32())  # List boundaries
list_array = pa.ListArray.from_arrays(offsets, struct_array)

print("List array (first element has null struct):")
print(list_array)
print(f"Type: {list_array.type}")
print()

# Create batch and try to write
batch = pa.record_batch([pa.array([0, 1]), list_array], names=["id", "value"])

print("Batch:")
print(batch)
print()

with tempfile.TemporaryDirectory(prefix="lance-null-elem-") as tmp:
    try:
        print("Writing to Lance...")
        ds = lance.write_dataset(batch, Path(tmp) / "ds")
        print("✅ Write successful!")

        print("\nReading back...")
        result = ds.to_table()
        print("✅ Read successful!")
        print("\nResult:")
        print(result)

    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback

        traceback.print_exc()
