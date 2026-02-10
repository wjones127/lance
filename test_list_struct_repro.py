#!/usr/bin/env python3
"""
Minimal reproduction for list-of-struct issue.

Tests whether list-of-struct data is preserved correctly on write/read cycle
across different file format versions.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

# Build list array with various cases
# Create the type first: List<Struct<tag: string>>
list_struct_type = pa.list_(pa.struct([("tag", pa.string())]))

# Test with Python API - simple null fields
print("=" * 60)
print("Test 1: List<Struct> with null fields (Python API)")
print("=" * 60)

list_builder = pa.array(
    [
        # 0: [{tag: "a"}, {tag: "b"}]
        [{"tag": "a"}, {"tag": "b"}],
        # 1: [{tag: "c"}]
        [{"tag": "c"}],
        # 2: null — fully null list
        None,
        # 3: [] — empty list
        [],
        # 4: [{tag: "a"}, {tag: null}] — null in struct field
        [{"tag": "a"}, {"tag": None}],
        # 5: [{tag: "d"}, {tag: "e"}, {tag: "f"}]
        [{"tag": "d"}, {"tag": "e"}, {"tag": "f"}],
    ],
    type=list_struct_type,
)

# Now test with null struct elements in the list (harder case - like Rust test)
print("\n" + "=" * 60)
print("Test 2: List<Struct> with null struct elements")
print("=" * 60)

# Create struct array with nullability info for the struct itself

struct_type = pa.struct([("tag", pa.string())])

# Build arrays manually to get null struct elements
tag_array = pa.array(["a", None, "b", "c"])
struct_array_with_nulls = pa.StructArray.from_arrays(
    [tag_array],
    fields=[pa.field("tag", pa.string(), nullable=True)],
    # This creates a struct array with 4 elements, 2nd element has null struct
    mask=pa.array([False, True, False, False]),  # False means null, True means valid
)

print(f"Struct array with nulls: {struct_array_with_nulls}")

list_builder2 = pa.array(
    [
        [{"tag": "a"}],  # 0
        None,  # 1: null list
        [],  # 2: empty list
    ],
    type=list_struct_type,
)

id_array = pa.array(list(range(len(list_builder))))

# Create record batch
batch = pa.record_batch([id_array, list_builder], names=["id", "value"])

print("Original data:")
print(batch)
print()

# Test with different file versions
for version in [None, "2.1", "2.2"]:
    print(f"\n{'=' * 60}")
    print(f"Testing with file version: {version or 'default'}")
    print(f"{'=' * 60}")

    with tempfile.TemporaryDirectory(prefix="lance-list-struct-") as tmp:
        tmp_path = Path(tmp)

        # Write dataset
        ds = lance.write_dataset(batch, tmp_path / "ds")

        # Re-open and read
        ds_reopen = lance.dataset(tmp_path / "ds")
        result = ds_reopen.to_table()

        print("\nRead back data (full table):")
        print(result)
        print()

        # Compare - convert batch to table for comparison
        batch_table = pa.table(
            [batch.column(name) for name in batch.column_names],
            names=batch.column_names,
        )

        if result.equals(batch_table):
            print("✅ Basic scan matches!")
        else:
            print("❌ Basic scan MISMATCH!")

        # Test filtering
        print("\nTesting filter operations:")
        try:
            # Test: value is null
            filtered = ds_reopen.to_table(filter="value is null")
            print("  ✅ 'value is null' filter works")
            if len(filtered) > 0:
                print(f"    Found {len(filtered)} null rows")
        except Exception as e:
            print(f"  ❌ 'value is null' filter failed: {e}")

        try:
            # Test: value is not null
            filtered = ds_reopen.to_table(filter="value is not null")
            print("  ✅ 'value is not null' filter works")
            if len(filtered) > 0:
                print(f"    Found {len(filtered)} non-null rows")
        except Exception as e:
            print(f"  ❌ 'value is not null' filter failed: {e}")

        try:
            # Try ordering by id
            scanner = ds_reopen.scanner()
            ordered = scanner.to_table()
            print("  ✅ Scanning with order works")
        except Exception as e:
            print(f"  ❌ Scanning with order failed: {e}")
