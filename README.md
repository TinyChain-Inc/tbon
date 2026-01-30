# Tinychain Binary Object Notation

Tinychain Binary Object Notation (TBON) is a compact and versatile stream-friendly binary serialization format.

## Compatibility notes

- See `CHANGELOG.md` for behavior and dependency changes.
- TBON is a binary format (not JSON) and is designed to be portable across architectures.
- Duplicate map keys are not rejected; behavior depends on the target type.
- Decoding enforces a maximum nesting depth of 1024 by default; use
  `tbon::de::decode_with_max_depth`/`tbon::de::try_decode_with_max_depth` (and
  `tbon::de::read_from_with_max_depth` with `tokio-io`) to override.
- There are no explicit size limits; hostile inputs may require significant CPU/memory.
- Decoding is strict about consuming the entire input stream: trailing bytes after the first value
  are treated as an error. To encode multiple values, wrap them in a tuple/list/map.
- Default `destream` impl conventions used by this codec:
  - `i128`/`u128` encode as strings; decode accepts either strings or in-range integer tokens
  - `Duration` encodes as `(secs, nanos)` with `nanos < 1_000_000_000`

Example:
```rust
let expected = ("one".to_string(), 2.0, vec![3, 4], Bytes::from(vec![5u8]));
let stream = tbon::en::encode(&expected).unwrap();
let actual = tbon::de::try_decode((), stream).await.unwrap();
assert_eq!(expected, actual);
```

Example (multiple values):
```rust
let expected = vec![
    ("one".to_string(), 2.0),
    ("two".to_string(), 3.0),
];

let stream = tbon::en::encode(&expected).unwrap();
let actual = tbon::de::try_decode((), stream).await.unwrap();
assert_eq!(expected, actual);
```

## Chunk-size micro-benchmark

To inspect decode performance sensitivity to input chunk size:

`cargo test --test bench_chunk_size -- --ignored --nocapture`

## Criterion benchmark

For more stable measurements (and throughput reporting):

`cargo bench --bench chunk_size`

## Buffered encoding

If your transport does one write per stream item, buffering encoder output can reduce chunk count:

- `tbon::en::encode_buffered(value, 1024)`
