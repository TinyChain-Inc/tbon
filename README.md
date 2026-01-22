# Tinychain Binary Object Notation

Tinychain Binary Object Notation (TBON) is a compact and versatile stream-friendly binary serialization format.

## Compatibility notes

- See `CHANGELOG.md` for behavior and dependency changes.
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

## Chunk-size micro-benchmark

To inspect decode performance sensitivity to input chunk size:

`cargo test --test bench_chunk_size -- --ignored --nocapture`
