# Tinychain Binary Object Notation

Tinychain Binary Object Notation (TBON) is a compact and versatile stream-friendly binary serialization format.

## Encoding and limits

- TBON is a binary format designed to be portable across architectures.
- Duplicate map keys are not rejected; behavior depends on the target type.
- Decoding enforces a maximum nesting depth of 1024 by default; use
  `tbon::de::decode_with_max_depth`/`tbon::de::try_decode_with_max_depth` (and
  `tbon::de::read_from_with_max_depth` with `tokio-io`) to override.
- Nesting limits do not bound total input size or decoded allocations; callers
  own those budgets.
- Decoding is strict about consuming the entire input stream: trailing bytes after the first value
  are treated as an error. To encode multiple values, wrap them in a tuple/list/map.
- Default `destream` impl conventions used by this codec:
  - `i128`/`u128` encode as strings; decode accepts either strings or in-range integer tokens
  - `Duration` encodes as `(secs, nanos)` with `nanos < 1_000_000_000`

Example:
```rust
use bytes::Bytes;

let expected = ("one".to_string(), 2.0, vec![3, 4], Bytes::from(vec![5u8]));
let stream = tbon::en::encode(&expected).unwrap();
let actual: (String, f64, Vec<i32>, Bytes) = tbon::de::try_decode((), stream).await.unwrap();
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

## Structural inspection

`destream::de::Decoder::inspect_any` shares the strict ignored-value traversal
and configured nesting limit. It visits bounded text chunks, typed-array lengths,
and container cardinalities without constructing decoded payloads. Value owners
interpret those observations for allocation admission. The decoder retains the
current input chunk by handle and copies at most 4 KiB per refill; typed arrays
are consumed incrementally during inspection.

For input-dependent nesting, follow `destream`'s
[iterative traversal guidance](https://docs.rs/destream/latest/destream/).
Container cursors and inspection share framing operations and the configured
nesting limit.
