use std::cmp::min;
use std::time::{Duration, Instant};

use bytes::Bytes;
use destream::FromStream;
use futures::future;
use futures::stream;
use futures::TryStreamExt;

fn chunk_stream(bytes: Bytes, chunk_size: usize) -> impl futures::Stream<Item = Bytes> + Unpin {
    assert!(chunk_size > 0);

    let len = bytes.len();
    let chunks = (0..len)
        .step_by(chunk_size)
        .map(move |i| bytes.slice(i..min(i + chunk_size, len)));

    stream::iter(chunks)
}

#[derive(Clone)]
struct U64Array {
    data: Vec<u64>,
}

struct U64ArrayVisitor;

impl destream::de::Visitor for U64ArrayVisitor {
    type Value = U64Array;

    fn expecting() -> &'static str {
        "a u64 array"
    }

    async fn visit_array_u64<A: destream::de::ArrayAccess<u64>>(
        self,
        mut array: A,
    ) -> Result<Self::Value, A::Error> {
        let mut data = Vec::new();
        let mut buffer = [0_u64; 1024];
        loop {
            let num_items = array.buffer(&mut buffer).await?;
            if num_items == 0 {
                break;
            }
            data.extend_from_slice(&buffer[..num_items]);
        }

        Ok(U64Array { data })
    }
}

impl FromStream for U64Array {
    type Context = ();

    async fn from_stream<D: destream::de::Decoder>(
        _: (),
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        decoder.decode_array_u64(U64ArrayVisitor).await
    }
}

impl<'en> destream::en::ToStream<'en> for U64Array {
    fn to_stream<E: destream::en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        encoder.encode_array_u64(futures::stream::once(future::ready(self.data.clone())))
    }
}

async fn encode_payload(value: &U64Array) -> Bytes {
    let stream = tbon::en::encode(value).expect("encode tbon payload");
    let encoded: Vec<u8> = stream
        .try_fold(Vec::new(), |mut buf, chunk| {
            buf.extend_from_slice(&chunk);
            future::ready(Ok(buf))
        })
        .await
        .expect("encode fold");

    Bytes::from(encoded)
}

async fn decode_once(payload: Bytes, chunk_size: usize) -> Duration {
    let started = Instant::now();
    let decoded: U64Array = tbon::de::decode((), chunk_stream(payload, chunk_size))
        .await
        .expect("decode tbon payload");
    std::hint::black_box(decoded.data.len());
    started.elapsed()
}

// Minimal, dependency-free micro-benchmark to validate decode sensitivity to input chunk size.
//
// Run with:
// - `cargo test --test bench_chunk_size -- --ignored --nocapture`
//
// Optionally enable a coarse regression check with:
// - `TBON_BENCH_ASSERT=1 cargo test --test bench_chunk_size -- --ignored --nocapture`
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn bench_decode_chunk_sizes() {
    let value = U64Array {
        data: (0..100_000).map(|i| i as u64).collect(),
    };
    let payload = encode_payload(&value).await;

    let chunk_sizes = [1_usize, 8, 64, 1024, 8192, 65536];
    let iterations = 5_usize;

    eprintln!(
        "tbon chunk-size bench: payload={} bytes, iterations={}",
        payload.len(),
        iterations
    );

    let mut results = Vec::with_capacity(chunk_sizes.len());
    for &chunk_size in &chunk_sizes {
        let mut total = Duration::ZERO;
        for _ in 0..iterations {
            total += decode_once(payload.clone(), chunk_size).await;
        }

        let avg = total / (iterations as u32);
        eprintln!("  chunk_size={chunk_size:>6} avg={avg:?}");
        results.push((chunk_size, avg));
    }

    if std::env::var_os("TBON_BENCH_ASSERT").is_some() {
        let slowest = results.iter().max_by_key(|(_, d)| *d).unwrap();
        let fastest = results.iter().min_by_key(|(_, d)| *d).unwrap();

        eprintln!(
            "  fastest: chunk_size={} avg={:?}\n  slowest: chunk_size={} avg={:?}",
            fastest.0, fastest.1, slowest.0, slowest.1
        );

        assert!(
            slowest.1 <= fastest.1 * 5,
            "unexpected chunk-size sensitivity: slowest {:?} vs fastest {:?}",
            slowest,
            fastest
        );
    }
}
