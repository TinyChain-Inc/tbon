use bytes::Bytes;
use futures::executor::block_on;
use futures::{stream, TryStreamExt};

#[test]
fn duration_requires_exact_cardinality_and_termination() {
    use std::time::Duration;

    block_on(async {
        let expected = vec![Duration::new(5, 7)];
        let actual: Vec<Duration> =
            crate::de::try_decode((), crate::en::encode(&expected).unwrap())
                .await
                .unwrap();
        assert_eq!(actual, expected);

        let chunks: Vec<Bytes> = crate::en::encode(Duration::new(5, 7))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut truncated: Vec<u8> = chunks
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect();
        assert_eq!(truncated.pop(), Some(crate::constants::LIST_END[0]));
        for chunk in [1, 2, 7] {
            let result = crate::de::decode::<_, Duration>(
                (),
                stream::iter(truncated.chunks(chunk).map(Bytes::copy_from_slice)),
            )
            .await;
            assert!(result.is_err());
        }
        assert!(crate::de::try_decode::<_, _, Duration>(
            (),
            crate::en::encode((5u64, 7u32, 9u8)).unwrap(),
        )
        .await
        .is_err());
        let baseline = crate::de::try_decode::<_, _, Duration>(
            (),
            crate::en::encode((5u64, 1_000_000_000u32)).unwrap(),
        )
        .await
        .unwrap_err();
        let extra = crate::de::try_decode::<_, _, Duration>(
            (),
            crate::en::encode((5u64, 1_000_000_000u32, 9u8)).unwrap(),
        )
        .await
        .unwrap_err();
        assert_eq!(extra.to_string(), baseline.to_string());
    });
}

#[test]
fn truncated_arrays_and_invalid_boolean_representations_are_errors() {
    block_on(async {
        let encoded: Vec<Bytes> = crate::en::encode(Bytes::from_static(&[1, 2, 3]))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let wire: Vec<u8> = encoded.iter().flat_map(|b| b.iter().copied()).collect();
        for length in 0..wire.len() {
            let source = stream::iter(wire[..length].iter().map(|&b| Bytes::from(vec![b])));
            assert!(crate::de::decode::<_, Bytes>((), source).await.is_err());
        }
        let mut encoded: Vec<Bytes> = crate::en::encode(true)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut wire: Vec<u8> = encoded.drain(..).flat_map(|b| b.to_vec()).collect();
        *wire.last_mut().unwrap() = 2;
        assert!(
            crate::de::decode::<_, bool>((), stream::iter([Bytes::from(wire)]))
                .await
                .is_err()
        );
        let encoded = crate::en::encode(SignedBytes(vec![-128i8, 0, 127])).unwrap();
        let values: SignedBytes = crate::de::try_decode((), encoded).await.unwrap();
        assert_eq!(values.0, [-128, 0, 127]);

        let encoded = crate::en::encode(vec![1u8, 2, 3]).unwrap();
        let result: Result<[u8; 2], _> = crate::de::try_decode((), encoded).await;
        assert!(result.is_err());
    });
}

struct SignedBytes(Vec<i8>);

impl<'en> destream::IntoStream<'en> for SignedBytes {
    fn into_stream<E: destream::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        encoder.encode_array_i8(stream::iter([self.0]))
    }
}

impl destream::FromStream for SignedBytes {
    type Context = ();

    async fn from_stream<D: destream::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct ReadSigned;

        impl destream::Visitor for ReadSigned {
            type Value = SignedBytes;

            fn expecting() -> &'static str {
                "signed-byte array"
            }

            async fn visit_array_i8<A: destream::de::ArrayAccess<i8>>(
                self,
                mut array: A,
            ) -> Result<SignedBytes, A::Error> {
                let mut values = Vec::new();
                let mut buffer = [0; 3];
                loop {
                    let len = array.buffer(&mut buffer).await?;
                    if len == 0 {
                        break;
                    }
                    values.extend_from_slice(&buffer[..len]);
                }

                Ok(SignedBytes(values))
            }
        }

        decoder.decode_any(ReadSigned).await
    }
}

#[test]
fn uuid_arrays_require_exact_cardinality_and_a_terminator() {
    block_on(async {
        for len in [15, 17] {
            let encoded = crate::en::encode(Bytes::from(vec![0; len])).unwrap();
            let decoded: Result<uuid::Uuid, _> = crate::de::try_decode((), encoded).await;
            assert!(decoded.is_err());
        }
        let encoded = crate::en::encode(Bytes::from(vec![0; 16])).unwrap();
        let decoded: uuid::Uuid = crate::de::try_decode((), encoded).await.unwrap();
        assert!(decoded.is_nil());
    });
}
