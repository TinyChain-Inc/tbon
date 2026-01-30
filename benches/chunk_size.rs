use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

mod common;

fn bench_decode_chunk_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let value = common::U64Array {
        data: (0..100_000).map(|i| i as u64).collect(),
    };
    let payload = rt.block_on(common::encode_payload(&value));
    let chunk_sizes = [1_usize, 8, 64, 1024, 8192, 65536];

    let mut group = c.benchmark_group("decode_chunk_size");
    group.throughput(Throughput::Bytes(payload.len() as u64));

    for &chunk_size in &chunk_sizes {
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &chunk_size,
            |b, &cs| {
                let bytes = payload.clone();
                b.iter(|| {
                    let decoded: common::U64Array = rt
                        .block_on(tbon::de::decode(
                            (),
                            common::chunk_stream(bytes.clone(), cs),
                        ))
                        .expect("decode tbon payload");
                    std::hint::black_box(decoded.data.len());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_decode_chunk_sizes);
criterion_main!(benches);
