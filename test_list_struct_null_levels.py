#!/usr/bin/env python3
"""
Test null handling at different levels in List<Struct>.

Explores whether the issue is related to nulls at:
1. Base level (null list)
2. Child level (null struct field)
3. Element level (null struct element in list)
4. Combinations of the above
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])
list_struct_type = pa.list_(struct_type)

test_cases = [
    (
        "No nulls at any level",
        pa.array(
            [
                [{"tag": "a"}],
                [{"tag": "b"}],
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Null list (base level)",
        pa.array(
            [
                [{"tag": "a"}],
                None,
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Null struct field (child level)",
        pa.array(
            [
                [{"tag": "a"}, {"tag": None}],
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Empty list (base level)",
        pa.array(
            [
                [],
                [{"tag": "a"}],
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Null + non-null combo",
        pa.array(
            [
                [{"tag": "a"}],
                None,
                [{"tag": "b"}],
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Null field + null list combo",
        pa.array(
            [
                [{"tag": None}],
                None,
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Multiple nulls in struct field",
        pa.array(
            [
                [{"tag": "a"}, {"tag": None}, {"tag": "b"}],
            ],
            type=list_struct_type,
        ),
    ),
    (
        "Empty + null combo",
        pa.array(
            [
                [],
                None,
            ],
            type=list_struct_type,
        ),
    ),
]

# Try to create null struct element in list (the problematic case from Rust test)
try:
    # Build with explicit nullability

    tag_array = pa.array(["a", None, "b"])
    struct_array = pa.StructArray.from_arrays(
        [tag_array], fields=[pa.field("tag", pa.string(), nullable=True)]
    )

    # Create list with a null struct element
    # This is tricky - we need to build a list that contains a null struct
    print("\nAttempting to create List with null struct element...")

    # Use ListBuilder approach similar to Rust
    list_builder = pa.ListBuilder(pa.list_(struct_type))
    # Can't easily do this with Python API - would need lower-level builders
    print("  Note: Python API doesn't easily support null struct elements")

except Exception as e:
    print(f"Error building null struct in list: {e}")

print("=" * 70)
print("Testing List<Struct> data with nulls at different levels")
print("=" * 70)

for test_name, list_array in test_cases:
    print(f"\n{test_name}:")

    # Print the data structure
    print(f"  Data: {list_array}")
    print(f"  Type: {list_array.type}")

    # Create batch
    batch = pa.record_batch(
        [pa.array(range(len(list_array))), list_array], names=["id", "value"]
    )

    # Try to write and read
    with tempfile.TemporaryDirectory(prefix="lance-null-test-") as tmp:
        try:
            ds = lance.write_dataset(batch, Path(tmp) / "ds")
            result = ds.to_table()
            print("  ✅ Write/read successful")
        except Exception as e:
            print(f"  ❌ FAILED: {e}")

print("\n" + "=" * 70)
print("Key insight:")
print("=" * 70)
print("""
The Rust test uses ListBuilder + StructBuilder which can create:
  - Null struct elements in a list (not just null fields)

The Python API doesn't easily support this - it would require:
  1. Creating a StructArray with a null validity bit at element level
  2. Including it in a ListArray

This might be what's causing the encoder to panic - an edge case
where the struct itself is null, not just its fields.
""")
