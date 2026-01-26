# Schema Specification

## Overview

A Lance schema defines the structure of data stored in a Lance table. Schemas
consist of fields that represent columns, including nested fields for complex types.
Each field in the schema has a unique integer ID.

<details>
<summary>Schema protobuf message</summary>

```protobuf
%%% proto.message.lance.file.Schema %%%
```
</details>

## Data Types

Lance represents data types as strings called "logical types" for serialization.
The following table shows the mapping between logical type strings and Arrow data types.

### Primitive Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `null` | Null | Null type (no values) |
| `bool` | Boolean | True or false |
| `int8` | Int8 | 8-bit signed integer |
| `uint8` | UInt8 | 8-bit unsigned integer |
| `int16` | Int16 | 16-bit signed integer |
| `uint16` | UInt16 | 16-bit unsigned integer |
| `int32` | Int32 | 32-bit signed integer |
| `uint32` | UInt32 | 32-bit unsigned integer |
| `int64` | Int64 | 64-bit signed integer |
| `uint64` | UInt64 | 64-bit unsigned integer |
| `halffloat` | Float16 | 16-bit floating point |
| `float` | Float32 | 32-bit floating point |
| `double` | Float64 | 64-bit floating point |

### String and Binary Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `string` | Utf8 | UTF-8 string (32-bit offsets) |
| `large_string` | LargeUtf8 | UTF-8 string (64-bit offsets) |
| `binary` | Binary | Binary data (32-bit offsets) |
| `large_binary` | LargeBinary | Binary data (64-bit offsets) |
| `fixed_size_binary:{size}` | FixedSizeBinary(size) | Fixed-size binary data |

### Temporal Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `date32:day` | Date32 | Days since Unix epoch |
| `date64:ms` | Date64 | Milliseconds since Unix epoch |
| `time32:s` | Time32(Second) | Time of day (seconds) |
| `time32:ms` | Time32(Millisecond) | Time of day (milliseconds) |
| `time64:us` | Time64(Microsecond) | Time of day (microseconds) |
| `time64:ns` | Time64(Nanosecond) | Time of day (nanoseconds) |
| `timestamp:{unit}:{tz}` | Timestamp(unit, tz) | Timestamp with optional timezone |
| `duration:{unit}` | Duration(unit) | Time duration |

For timestamps, `{unit}` is one of `s`, `ms`, `us`, `ns`.
The `{tz}` is the timezone string or `-` for no timezone.

Examples:

- `timestamp:us:-` is `Timestamp(Microsecond, None)`
- `timestamp:ns:America/New_York` is `Timestamp(Nanosecond, Some("America/New_York"))`

### Decimal Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `decimal:128:{precision}:{scale}` | Decimal128(precision, scale) | 128-bit decimal |
| `decimal:256:{precision}:{scale}` | Decimal256(precision, scale) | 256-bit decimal |

Examples:

- `decimal:128:10:2` is `Decimal128(10, 2)` (10 total digits, 2 after decimal point)

### Dictionary Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `dict:{value_type}:{key_type}:false` | Dictionary(key_type, value_type) | Dictionary-encoded values |

The key type is typically an integer type.
The `false` indicates unordered dictionary values.

Examples:

- `dict:string:int32:false` is `Dictionary(Int32, Utf8)`

### Collection Types

| Logical Type | Arrow Type | Description |
|-------------|------------|-------------|
| `list` | List | Variable-length list |
| `list.struct` | List&lt;Struct&gt; | List of structs |
| `large_list` | LargeList | List with 64-bit offsets |
| `large_list.struct` | LargeList&lt;Struct&gt; | Large list of structs |
| `fixed_size_list:{elem_type}:{size}` | FixedSizeList(elem_type, size) | Fixed-length list |
| `map` | Map | Key-value map |
| `struct` | Struct | Nested struct |

Examples:

- `fixed_size_list:float:128` is `FixedSizeList(Float32, 128)` (common for embedding vectors)
- `fixed_size_list:struct:4` is a fixed-size list of 4 struct elements

### Extension Types

Lance supports Arrow extension types through field metadata.
The extension type name is stored in `ARROW:extension:name` and extension metadata in `ARROW:extension:metadata`.

| Logical Type | Extension | Description |
|-------------|-----------|-------------|
| `blob` | lance.blob | Large binary objects stored separately |
| `json` | lance.json | JSON data stored as binary |

## Physical Encodings

Lance encodes values using a small set of physical buffer layouts. Multiple
logical types share the same physical layout, allowing the encoding system to
optimize based on data characteristics rather than semantic type. These layouts
generally match the [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
buffer layouts.

### Fixed-Width Primitives

Integer and floating-point types are stored as a contiguous buffer of fixed-size
elements. Each value occupies a fixed number of bytes determined by the type.

| Logical Types | Bytes per Value |
|---------------|-----------------|
| bool | 1 bit (packed) |
| int8, uint8 | 1 |
| int16, uint16, halffloat | 2 |
| int32, uint32, float | 4 |
| int64, uint64, double | 8 |

Boolean values are bit-packed, with 8 values per byte.

### Temporal Types

Temporal types use the same fixed-width layout as integers, storing values as
integer offsets from an epoch or start of day.

| Logical Type | Storage | Interpretation |
|--------------|---------|----------------|
| date32 | int32 | Days since Unix epoch |
| date64 | int64 | Milliseconds since Unix epoch |
| time32 | int32 | Seconds or milliseconds since midnight |
| time64 | int64 | Microseconds or nanoseconds since midnight |
| timestamp | int64 | Time units since Unix epoch |
| duration | int64 | Time units |

### Decimal Types

Decimal types are stored as fixed-width little-endian integers.

| Logical Type | Bytes per Value |
|--------------|-----------------|
| decimal:128 | 16 |
| decimal:256 | 32 |

### Variable-Width Types

String and binary types are stored using two buffers: a data buffer containing
concatenated values, and an offsets buffer indicating where each value starts.

| Component | Description |
|-----------|-------------|
| data | Concatenated UTF-8 strings or binary values |
| offsets | Array of byte positions (length = num_values + 1) |

The offset type determines the maximum total data size:

| Logical Types | Offset Type | Max Data Size |
|---------------|-------------|---------------|
| string, binary | int32 | 2 GB |
| large_string, large_binary | int64 | 8 EB |

The physical encoding is identical for string vs binary and large_string vs
large_binary - only the interpretation (UTF-8 text vs arbitrary bytes) and
offset width differ.

### Fixed-Size Binary

Fixed-size binary is stored as a contiguous buffer where each value occupies
exactly `size` bytes, similar to fixed-width primitives.

### List Types

List types use **repetition levels** to encode the list structure, inspired by the
[Dremel paper](https://research.google/pubs/pub36632/). Child elements are stored
in a flattened buffer, with repetition levels indicating where each list begins.

Repetition levels are integers that mark list boundaries at each nesting depth.
This encoding handles arbitrary nesting and empty lists efficiently. The
distinction between `list` and `large_list` affects only the Arrow reconstruction,
not the physical encoding.

### Fixed-Size List Types

Fixed-size lists do not require repetition levels since all lists have the same
length. Values are stored as a flattened buffer of child elements.

For `fixed_size_list:{type}:{size}`, row `i` contains elements at positions
`[i * size, (i + 1) * size)` in the values buffer.

### Struct Types

Struct fields are encoded as separate columns, one for each child field. There
is no physical "struct buffer" - the struct exists only as a logical grouping
of its children.

### Map Types

Map types are encoded like lists, using repetition levels to encode the map
structure. Each map entry is a struct with `key` and `value` fields.

Keys must be non-nullable.

### Dictionary Types

Dictionary-encoded values are stored using two components:

| Component | Description |
|-----------|-------------|
| indices | Integer indices into the dictionary |
| dictionary | Unique values encoded according to their type |

The index type (int8, int16, int32, int64) determines the maximum dictionary
size. Indices reference positions in the dictionary buffer.

### Nullability

Nullability is encoded using **definition levels**, which indicate at what depth
a value becomes null. This approach (inspired by the Dremel paper) efficiently
combines nullability information from multiple nesting levels into a single buffer.

For non-nested nullable fields, definition levels are equivalent to an inverted
validity bitmap. For nested structures, definition levels compress multiple
validity bitmaps into one.

## Field Structure

Each field in the schema has the following properties.

<details>
<summary>Field protobuf message</summary>

```protobuf
%%% proto.message.lance.file.Field %%%
```

</details>

### Field Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Field name |
| `id` | int32 | Unique field identifier |
| `parent_id` | int32 | Parent field ID (-1 for top-level) |
| `logical_type` | string | String representation of Arrow type |
| `nullable` | bool | Whether field allows null values |
| `metadata` | map&lt;string, bytes&gt; | Optional field metadata |

### Field Types in Protobuf

Fields are classified into three types based on their structure:

- **PARENT**: Struct fields that contain child fields. Always have logical type `struct`.
- **REPEATED**: List fields that contain repeated elements. Logical types include `list`, `large_list`, `list.struct`, `large_list.struct`.
- **LEAF**: Primitive fields with no children. All other logical types.

## Field IDs

Field IDs are unique integer identifiers assigned to each field in the schema.
They enable schema evolution by providing stable references to fields across table versions.

### Assignment Rules

1. **Initial assignment**: When a table is first created, field IDs are assigned in depth-first order starting from 0.
2. **New fields**: When columns are added, new field IDs are assigned incrementally based on the maximum existing ID.
3. **Nested fields**: Each nested field (struct children, list elements) receives its own unique ID.
4. **Top-level fields**: Have `parent_id = -1`.
5. **Child fields**: Have `parent_id` set to their parent's field ID.

### Special Values

| Value | Meaning |
|-------|---------|
| `-1` | Default/unassigned ID, or parent_id for top-level fields |
| `-2` | Tombstone value indicating a removed field |

### Tombstoning

When a column is dropped or replaced, its field can be removed from the schema.
However, data files have a list of fields they contain based on field IDs. In this
list, the dropped field's ID is marked as tombstoned with a value of `-2`.
This allows older data files to remain valid while the field is ignored in newer reads.

### Example

Consider a schema with the following structure:

```
id: int64
name: string
location: struct
  lat: float64
  lon: float64
```

Field IDs would be assigned as:

| Field | ID | Parent ID |
|-------|-----|-----------|
| id | 0 | -1 |
| name | 1 | -1 |
| location | 2 | -1 |
| location.lat | 3 | 2 |
| location.lon | 4 | 2 |

## Schema Evolution

Field IDs enable efficient schema evolution without rewriting data files.

### Supported Operations

- **Add columns**: New fields receive new IDs. Old data files don't contain these columns (read as NULL).
- **Drop columns**: Field IDs are tombstoned (`-2`). Data remains in files but is ignored.
- **Rename columns**: Field names change but IDs remain the same.
- **Reorder columns**: Field order changes but IDs remain the same.

### Data File Compatibility

Each data file records which field IDs it contains.
When reading, the reader uses field IDs to match columns from different data files, even if schemas have evolved between writes.

## Blob Columns

Blob columns store large binary objects that are too big for efficient columnar storage.
Instead of storing data inline, blob columns store descriptors pointing to the actual data.

### Blob v1 Format

Blob v1 columns are marked with metadata key `lance-schema:blob` set to `true`.
The descriptor is a struct with two fields:

| Field | Type | Description |
|-------|------|-------------|
| `position` | uint64 | Byte offset in the data file |
| `size` | uint64 | Size in bytes |

### Blob v2 Format

Blob v2 columns use the Arrow extension type `lance.blob.v2`.
The descriptor is a struct with five fields:

| Field | Type | Description |
|-------|------|-------------|
| `kind` | uint8 | Storage kind (0=inline, 1=packed, 2=dedicated, 3=external) |
| `position` | uint64 | Byte offset |
| `size` | uint64 | Size in bytes |
| `blob_id` | uint32 | Blob file identifier |
| `blob_uri` | utf8 | External URI (for kind=3) |
