use std::cmp::min;

use bytes::Bytes;
use destream::FromStream;
use futures::future;
use futures::stream;
use futures::TryStreamExt;

// Used across multiple benchmark entrypoints depending on enabled features.
#[allow(dead_code)] // used across multiple bench entrypoints depending on enabled features
pub fn chunk_stream(bytes: Bytes, chunk_size: usize) -> impl futures::Stream<Item = Bytes> + Unpin {
    assert!(chunk_size > 0);

    let len = bytes.len();
    let chunks = (0..len)
        .step_by(chunk_size)
        .map(move |i| bytes.slice(i..min(i + chunk_size, len)));

    stream::iter(chunks)
}

#[derive(Clone)]
pub struct U64Array {
    pub data: Vec<u64>,
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

// Used across multiple benchmark entrypoints depending on enabled features.
#[allow(dead_code)] // used across multiple bench entrypoints depending on enabled features
pub async fn encode_payload(value: &U64Array) -> Bytes {
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
