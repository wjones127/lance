#!/usr/bin/env python3
"""
Try to reproduce the repdef.rs:630 panic from the Rust test.

The Rust test uses ListBuilder + StructBuilder with a specific sequence:
1. [{tag: "a"}, {tag: "b"}]  - valid list with valid structs
2. [{tag: "c"}]              - valid list with valid struct
3. null                       - NULL LIST
4. []                         - EMPTY LIST
5. [{tag: "a"}, {tag: null}] - valid list with null struct field
6. [{tag: "d"}, {tag: "e"}, {tag: "f"}] - valid list
7. [null, {tag: "g"}]        - list with NULL STRUCT ELEMENT
...

The key sequences that might trigger the panic:
- null list followed by empty list
- null struct element in list
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])
list_struct_type = pa.list_(struct_type)

print("=" * 70)
print("Testing sequences that match Rust test structure")
print("=" * 70)

test_cases = [
    (
        "Null list + empty list",
        [
            [{"tag": "a"}],
            None,  # null list
            [],    # empty list
        ]
    ),
    (
        "Null list + empty list + null struct element",
        [
            [{"tag": "a"}],
            None,          # null list
            [],            # empty list
            [{"tag": "b"}],
            [None, {"tag": "c"}],  # null struct element
        ]
    ),
    (
        "Multiple empty lists with nulls",
        [
            [],
            None,
            [],
            [{"tag": "a"}],
        ]
    ),
    (
        "Exact Rust sequence (subset)",
        [
            [{"tag": "a"}, {"tag": "b"}],
            [{"tag": "c"}],
            None,
            [],
            [{"tag": "a"}],
            [{"tag": "d"}, {"tag": "e"}, {"tag": "f"}],
        ]
    ),
]

for test_name, list_data in test_cases:
    print(f"\n{test_name}:")
    print(f"  Data: {list_data}")

    try:
        list_array = pa.array(list_data, type=list_struct_type)
        batch = pa.record_batch(
            [pa.array(range(len(list_data))), list_array],
            names=["id", "value"]
        )

        with tempfile.TemporaryDirectory(prefix="lance-repdef-") as tmp:
            print("  Writing...")
            ds = lance.write_dataset(batch, Path(tmp) / "ds")
            print("  ✅ Write successful")

            print("  Reading...")
            result = ds.to_table()
            print("  ✅ Read successful")

    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        # Check if it's the repdef panic
        if "repdef" in str(e).lower() or "assertion" in str(e).lower():
            print("  🎯 FOUND THE REPDEF PANIC!")
            import traceback
            traceback.print_exc()

print("\n" + "=" * 70)
print("Testing with null struct ELEMENTS (most likely to trigger)")
print("=" * 70)

# Try to create null struct elements explicitly
try:
    # Build struct array with null struct elements at specific positions
    tag_array = pa.array(["a", "b", "c", "d", "e", "f"])
    struct_array = pa.StructArray.from_arrays(
        [tag_array],
        fields=[pa.field("tag", pa.string(), nullable=True)],
        # Pattern: null at positions 1 and 4
        mask=pa.array([True, False, True, True, False, True])
    )

    # Create list with specific boundaries to test different offset patterns
    # List 1: indices 0-1 (includes null at 1)
    # List 2: null
    # List 3: empty
    # List 4: indices 2-3
    # List 5: indices 4-5 (includes null at 4)
    offsets = pa.array([0, 2, 2, 2, 4, 6], type=pa.int32())

    list_array = pa.ListArray.from_arrays(offsets, struct_array)
    batch = pa.record_batch(
        [pa.array([0, 1, 2, 3, 4]), list_array],
        names=["id", "value"]
    )

    print("Struct array with null elements at indices 1, 4")
    print(f"List offsets: {offsets.to_pylist()}")

    with tempfile.TemporaryDirectory(prefix="lance-repdef-nullelem-") as tmp:
        print("Writing...")
        ds = lance.write_dataset(batch, Path(tmp) / "ds")
        print("✅ Write successful")

        print("Reading...")
        result = ds.to_table()
        print("✅ Read successful")

except Exception as e:
    print(f"❌ FAILED: {e}")
    if "repdef" in str(e).lower() or "assertion" in str(e).lower():
        print("🎯 FOUND THE REPDEF PANIC!")
    import traceback
    traceback.print_exc()
