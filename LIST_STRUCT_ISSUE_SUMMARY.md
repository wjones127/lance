# List<Struct> Data Persistence Issue

## Summary
The Rust test `test_query_list_struct` fails when writing and reading List<Struct> data, but the Python API works correctly for the same operations.

## Issue Details

### Failing Test
- `rust/lance/tests/query/nested.rs::test_query_list_struct`
- Fails with: `assertion failed` in `rust/lance-encoding/src/repdef.rs:630`
- Fails across all file format versions (default, 2.1, 2.2)

### Error
```
thread 'lance-cpu' panicked at rust/lance-encoding/src/repdef.rs:630:9:
assertion failed: self.current_len == 0 ||
    self.current_len == validity.len() + self.current_num_specials
```

### What Works
✅ Python API: Round-trip write/read with `List<Struct>` data
✅ Python API: Scanning and filtering `List<Struct>` columns
✅ Python API: All file format versions (default, 2.1, 2.2)

### What Fails
❌ Rust test framework: Writing `List<Struct>` data constructed with `ListBuilder` + `StructBuilder`
❌ The encoding layer panics before data can be written

## Root Cause Analysis

The issue appears to be in the encoding layer (`repdef.rs`), not in the persistence or reading logic. The panic occurs during the **write** operation, specifically when the encoding logic tries to validate the internal state.

This suggests:
1. The `ListBuilder` + `StructBuilder` construct creates a struct with specific validity/nullability semantics
2. The encoder makes assumptions that don't hold for this specific structure
3. The issue is not related to issue #838 (list-of-struct filtering/selection support)

## Differences from Issue #838

Issue #838 is about **filtering and selection** of list-of-struct columns not being properly handled.
This new issue is about **encoding/writing** list-of-struct data constructed a certain way failing completely.

These are likely two separate issues:
- **New Issue**: Encoding panic when writing List<Struct> with specific validity patterns
- **#838**: Filtering/selection operations on List<Struct> not working correctly

## Test Status

- `test_query_list_struct` - Panics on write (all versions)
- `test_query_list_struct_v2_1` - Panics on write (V2.1)
- `test_query_struct_v2_1` - **PASSES** (struct-level nulls ARE fixed in V2.1)
- `test_query_list_str` - Passes (LabelList disabled)
- `test_query_list_int` - Passes (LabelList disabled)

## Reproduction

### Python Reproduction (Works)
```python
import pyarrow as pa
import lance

list_struct_type = pa.list_(pa.struct([("tag", pa.string())]))
list_array = pa.array([
    [{"tag": "a"}, {"tag": "b"}],
    [{"tag": "c"}],
    None,
    [],
    [{"tag": "a"}, {"tag": None}],
], type=list_struct_type)

batch = pa.record_batch(
    [pa.array(range(5)), list_array],
    names=["id", "value"]
)

ds = lance.write_dataset(batch, "/tmp/test")
result = ds.to_table()  # Works fine!
```

### Rust Reproduction (Panics)
```rust
let mut builder = ListBuilder::new(StructBuilder::from_fields(...));
// ... build data ...
let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("value", value_array)]).unwrap();
DatasetTestCases::from_data(batch).with_file_version(LanceFileVersion::V2_1).run(...);
// Panics during write in repdef encoder
```

## Next Steps

1. Create new GitHub issue for the encoding panic
2. Separate this from issue #838
3. Investigate the repdef encoder to understand why the validity pattern causes a panic
4. Consider if this is a regression or long-standing issue
