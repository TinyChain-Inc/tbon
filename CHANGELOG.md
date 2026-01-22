# Changelog

## Unreleased

- Tighten `Duration` decoding to reject `nanos >= 1_000_000_000`.

## 0.8.0

- Upgrade to `destream 0.9`.
- Add conformance tests for extended `destream` default impl coverage (128-bit integers, `Duration`,
  and standard net address types).
