# Changelog

## Unreleased

- Tighten `Duration` decoding to reject `nanos >= 1_000_000_000`.
- Make decoding strict about trailing bytes (decoders now require end-of-stream after the first
  value).
- Fix decode performance sensitivity to input chunk size by avoiding `Vec` front-removals in the
  streaming buffer.
- Reduce allocations when decoding from `tokio::io::AsyncRead` by reusing an internal read buffer.
- Harden `IgnoredAny` decoding to reliably skip nested values (including lists, maps, and byte
  arrays) without recursion.
- Mitigate deep-nesting attacks by enforcing a maximum nesting depth of 1024 by default; expose
  `decode_with_max_depth`/`try_decode_with_max_depth` (and `read_from_with_max_depth` with
  `tokio-io`) to override.
- Add `tbon::en::encode_buffered`/`encode_seq_buffered`/`encode_map_buffered` to reduce the number
  of encoded output chunks (and downstream `await`s/writes) for common IO patterns.
- Chunk-size micro-benchmark (release build, `cargo test --test bench_chunk_size -- --ignored --nocapture`):
  - payload=801809 bytes, iterations=5
  - chunk_size=1 avg=26.252471ms; 8 avg=7.41426ms; 64 avg=6.266511ms; 1024 avg=6.030818ms;
    8192 avg=5.777171ms; 65536 avg=5.792528ms

## 0.8.0

- Upgrade to `destream 0.9`.
- Add conformance tests for extended `destream` default impl coverage (128-bit integers, `Duration`,
  and standard net address types).
