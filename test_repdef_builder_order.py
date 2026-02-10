#!/usr/bin/env python3
"""
Try to match the exact builder append pattern from the Rust test.

The key difference might be in HOW the data is built incrementally,
not just WHAT data ends up in the arrays.

Rust sequence:
1. list.append({a, b})     - valid list with 2 structs
2. list.append({c})        - valid list with 1 struct
3. list.append(null)       - null list
4. list.append([])         - empty list
5. list.append({a, null})  - valid list with struct having null field
...

The builders track state incrementally, which might cause different
offset/validity patterns than creating all data at once.
"""

import tempfile
from pathlib import Path
import pyarrow as pa
import lance

struct_type = pa.struct([("tag", pa.string())])

print("=" * 70)
print("Testing the exact append sequence from Rust test")
print("=" * 70)

# Try to match the incremental building pattern
# Instead of creating the full array, build lists incrementally

list_arrays = []

# 0: [{tag: "a"}, {tag: "b"}]
list_arrays.append([{"tag": "a"}, {"tag": "b"}])

# 1: [{tag: "c"}]
list_arrays.append([{"tag": "c"}])

# 2: null — fully null list
list_arrays.append(None)

# 3: [] — empty list
list_arrays.append([])

# 4: [{tag: "a"}, {tag: null}] — null in struct field
list_arrays.append([{"tag": "a"}, {"tag": None}])

# 5: [{tag: "d"}, {tag: "e"}, {tag: "f"}]
list_arrays.append([{"tag": "d"}, {"tag": "e"}, {"tag": "f"}])

# 6: [null, {tag: "g"}] — null struct element in list
# This is the critical one
list_arrays.append([None, {"tag": "g"}])

# 7: [{tag: "h"}]
list_arrays.append([{"tag": "h"}])

# 8: [{tag: "a"}, {tag: "a"}] — duplicate
list_arrays.append([{"tag": "a"}, {"tag": "a"}])

# 9: [{tag: "b"}]
list_arrays.append([{"tag": "b"}])

print(f"Total lists to create: {len(list_arrays)}")
for i, lst in enumerate(list_arrays):
    print(f"  {i}: {lst}")

# Try creating as a single array
try:
    list_array_type = pa.list_(struct_type)
    list_array = pa.array(list_arrays, type=list_array_type)

    batch = pa.record_batch(
        [pa.array(range(len(list_arrays))), list_array],
        names=["id", "value"]
    )

    print("\nAttempting to write full array...")
    with tempfile.TemporaryDirectory(prefix="lance-repdef-order-") as tmp:
        print("Writing...")
        ds = lance.write_dataset(batch, Path(tmp) / "ds")
        print("✅ Write successful")

        print("Reading...")
        result = ds.to_table()
        print("✅ Read successful")

except Exception as e:
    print(f"❌ FAILED: {e}")
    error_str = str(e)
    if "repdef" in error_str.lower():
        print("🎯 FOUND REPDEF PANIC!")
    if "assertion" in error_str.lower() and "current_len" in error_str:
        print("🎯 FOUND THE EXACT ASSERTION FROM RUST TEST!")
    if "630" in error_str:
        print("🎯 This is from repdef.rs:630!")
    import traceback
    traceback.print_exc()

# Also try building progressively with NULL struct element at the start of a list
print("\n" + "=" * 70)
print("Critical pattern: null list followed by list with null struct element")
print("=" * 70)

critical_pattern = [
    [{"tag": "a"}],          # normal list
    None,                    # null list - THIS CREATES SPECIAL HANDLING
    [],                      # empty list
    [None, {"tag": "b"}],    # null struct element at offset 0
]

print(f"Pattern: {critical_pattern}")

try:
    list_array = pa.array(critical_pattern, type=pa.list_(struct_type))
    batch = pa.record_batch(
        [pa.array(range(len(critical_pattern))), list_array],
        names=["id", "value"]
    )

    print("\nWriting...")
    with tempfile.TemporaryDirectory(prefix="lance-critical-") as tmp:
        ds = lance.write_dataset(batch, Path(tmp) / "ds")
        print("✅ Write successful")

        print("Reading...")
        result = ds.to_table()
        print("✅ Read successful")

except Exception as e:
    print(f"❌ FAILED: {e}")
    if "repdef" in str(e).lower() or "current_len" in str(e).lower():
        print("🎯 FOUND THE PANIC!")
    import traceback
    traceback.print_exc()
