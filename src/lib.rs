//! Library for encoding Rust program data into a binary stream, and decoding that stream.
//!
//! Example:
//! ```
//! # use futures::executor::block_on;
//! let expected = ("one".to_string(), 2.0, vec![3, 4], vec![5u8]);
//! let stream = tbon::en::encode(&expected).unwrap();
//! let actual = block_on(tbon::de::try_decode((), stream)).unwrap();
//! assert_eq!(expected, actual);
//! ```
//!
//! Compatibility notes:
//!  - Decoding enforces a maximum nesting depth of 1024 by default; use
//!    `tbon::de::decode_with_max_depth`/`tbon::de::try_decode_with_max_depth` to override.

// `tbon` implements `destream`'s `async fn` trait APIs on stable Rust, so we keep this `allow`
// until `async_fn_in_trait` is stabilized.
#![allow(async_fn_in_trait)]

use element::Element;

mod constants;
mod element;

pub mod de;
pub mod en;

#[cfg(test)]
mod tests {
    #![allow(clippy::approx_constant)] // tests use explicit literals to validate encoding/decoding

    use std::collections::{
        BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque,
    };
    use std::fmt;
    use std::iter::FromIterator;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::num::{NonZeroI128, NonZeroU128};
    use std::time::Duration;

    use bytes::Bytes;
    use destream::{FromStream, IntoStream};
    use futures::{future, stream, StreamExt, TryStreamExt};

    use rand::Rng;

    use super::constants::{
        Type, ARRAY_DELIMIT, ESCAPE, LIST_BEGIN, LIST_END, MAP_BEGIN, MAP_END, STRING_DELIMIT, TRUE,
    };
    use super::de::*;
    use super::en::*;
    use num_traits::Signed;
    use uuid::Uuid;

    async fn run_test<'en, T>(value: T)
    where
        T: FromStream<Context = ()> + IntoStream<'en> + fmt::Debug + PartialEq + Clone + 'en,
    {
        let encoded = encode(value.clone()).unwrap();
        let decoded: T = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, value);
    }

    async fn encode_to_vec<'en, T>(value: T) -> Vec<u8>
    where
        T: IntoStream<'en> + Clone + 'en,
    {
        encode(value)
            .unwrap()
            .try_fold(Vec::new(), |mut buffer, chunk| {
                buffer.extend_from_slice(&chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap()
    }

    async fn decode_from_chunks<T: FromStream<Context = ()>>(
        bytes: &[u8],
        chunk_size: usize,
    ) -> Result<T, super::de::Error> {
        let source = stream::iter(bytes.iter().copied())
            .chunks(chunk_size.max(1))
            .map(Bytes::from)
            .map(Result::<Bytes, super::en::Error>::Ok);

        try_decode((), source).await
    }

    async fn assert_decode_fails<'en, T, V>(value: V)
    where
        T: FromStream<Context = ()>,
        V: IntoStream<'en> + Clone + 'en,
    {
        let encoded = encode(value).unwrap();
        let result: Result<T, _> = try_decode((), encoded).await;
        assert!(result.is_err(), "expected decode to fail, but succeeded");
    }

    async fn assert_decode_bytes_fails<T: FromStream<Context = ()>>(bytes: &[u8]) {
        for chunk_size in 1..=bytes.len().clamp(1, 16) {
            let result: Result<T, _> = decode_from_chunks(bytes, chunk_size).await;
            assert!(result.is_err(), "expected decode to fail, but succeeded");
        }
    }

    #[tokio::test]
    async fn test_encode_buffered_equivalent() {
        let value = (true, -1i16, 3.14f64, "hello".to_string(), vec![1u8, 2, 3]);

        let baseline = encode_to_vec(value.clone()).await;
        let buffered_stream = super::en::encode_buffered(value, 1024).unwrap();
        let buffered: Vec<u8> = buffered_stream
            .try_fold(Vec::new(), |mut buffer, chunk| {
                buffer.extend_from_slice(&chunk);
                future::ready(Ok(buffer))
            })
            .await
            .unwrap();

        assert_eq!(baseline, buffered);
    }

    #[tokio::test]
    async fn test_encode_large_seq_no_stack_overflow() {
        let value: Vec<u64> = (0..100_000).map(|i| i as u64).collect();

        let encoded = encode_to_vec(value.clone()).await;
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], LIST_BEGIN[0]);
        assert_eq!(encoded[encoded.len() - 1], LIST_END[0]);
    }

    #[tokio::test]
    async fn test_encode_large_map_no_stack_overflow() {
        let value: BTreeMap<u64, u64> = (0..50_000_u64).map(|i| (i, i + 1)).collect();

        let encoded = encode_to_vec(value).await;
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], MAP_BEGIN[0]);
        assert_eq!(encoded[encoded.len() - 1], MAP_END[0]);
    }

    #[tokio::test]
    async fn test_decode_chunk_boundaries() {
        let value = (true, -1i16, 3.14f64, "hello".to_string(), vec![1u8, 2, 3]);

        let bytes = encode_to_vec(value.clone()).await;
        for chunk_size in 1..=bytes.len().clamp(1, 16) {
            let decoded: (bool, i16, f64, String, Vec<u8>) =
                decode_from_chunks(&bytes, chunk_size).await.unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[tokio::test]
    async fn test_decode_chunk_boundaries_with_escapes() {
        // Ensure escapes spanning chunk boundaries are handled:
        // - Strings escape `"` and `\\`
        // - Byte arrays escape `=` (array delimiter) and `\\`
        let value = (
            "this is a \"string\" within a \\ string".to_string(),
            Bytes::from(vec![b'=', b'\\', 1u8, 2u8, b'=', b'\\']),
        );

        let bytes = encode_to_vec(value.clone()).await;
        for chunk_size in 1..=bytes.len().clamp(1, 16) {
            let decoded: (String, Bytes) = decode_from_chunks(&bytes, chunk_size).await.unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[tokio::test]
    async fn test_truncated_escape_fails() {
        // string: '"' '\' EOF
        assert_decode_bytes_fails::<String>(&[STRING_DELIMIT[0], ESCAPE[0]]).await;

        // bytes: '=' <dtype> '\' EOF
        assert_decode_bytes_fails::<Vec<u8>>(&[ARRAY_DELIMIT[0], Type::U8 as u8, ESCAPE[0]]).await;
    }

    #[tokio::test]
    async fn test_unterminated_string_fails() {
        assert_decode_bytes_fails::<String>(&[STRING_DELIMIT[0], b'a', b'b']).await;
    }

    #[tokio::test]
    async fn test_invalid_utf8_string_fails() {
        assert_decode_bytes_fails::<String>(&[STRING_DELIMIT[0], 0xFF, STRING_DELIMIT[0]]).await;
    }

    #[tokio::test]
    async fn test_malformed_arrays_fail() {
        // unknown array dtype
        struct Any;
        struct AnyVisitor;

        impl destream::de::Visitor for AnyVisitor {
            type Value = Any;

            fn expecting() -> &'static str {
                "any TBON value"
            }

            fn visit_unit<E: destream::de::Error>(self) -> Result<Self::Value, E> {
                Ok(Any)
            }
        }

        impl FromStream for Any {
            type Context = ();

            async fn from_stream<D: destream::de::Decoder>(
                _: (),
                decoder: &mut D,
            ) -> Result<Self, D::Error> {
                decoder.decode_any(AnyVisitor).await
            }
        }

        assert_decode_bytes_fails::<Any>(&[ARRAY_DELIMIT[0], 0xFF]).await;

        // missing array end delimiter
        assert_decode_bytes_fails::<Vec<u8>>(&[ARRAY_DELIMIT[0], Type::U8 as u8, 1, 2, 3]).await;
    }

    #[tokio::test]
    async fn test_trailing_bytes_fail() {
        let bytes = [Type::Bool as u8, TRUE[0], 0xFF, 0xFF];
        let result: Result<bool, _> = decode_from_chunks(&bytes, 1).await;
        assert!(result.is_err(), "expected trailing bytes to cause an error");
    }

    #[tokio::test]
    async fn test_ignored_any_consumes_nested_values() {
        // IgnoredAny must be able to consume nested values, including arrays (Bytes) and strings.
        let value = (
            HashMap::<String, Vec<u8>>::from_iter([(
                "a".to_string(),
                vec![1u8, 2, 3, b'=', b'\\'],
            )]),
            vec![
                "hello".to_string(),
                "this is a \"string\" within a \\ string".to_string(),
            ],
            Bytes::from(vec![b'=', b'\\', 0, 1, 2, b'=', b'\\']),
            HashMap::<String, HashMap<String, Vec<u8>>>::from_iter([(
                "nested".to_string(),
                HashMap::from_iter([("k".to_string(), vec![9u8, 8, 7])]),
            )]),
        );

        let encoded = encode(&value).unwrap();
        let _: destream::IgnoredAny = try_decode((), encoded).await.unwrap();
    }

    #[tokio::test]
    async fn test_decode_ignored_any_deep_nesting() {
        // IgnoredAny must be able to consume deep nesting without recursion.
        const DEPTH: usize = 2048;
        let mut bytes = Vec::with_capacity(DEPTH * 2);
        bytes.extend(std::iter::repeat_n(LIST_BEGIN[0], DEPTH));
        bytes.extend(std::iter::repeat_n(LIST_END[0], DEPTH));

        let source = stream::iter(bytes.iter().copied())
            .chunks(1)
            .map(Bytes::from)
            .map(Result::<Bytes, super::en::Error>::Ok);

        let _: destream::IgnoredAny = super::de::try_decode_with_max_depth((), source, DEPTH + 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_decode_reject_too_deep_nesting() {
        const DEPTH: usize = 1025;
        let mut bytes = Vec::with_capacity(DEPTH * 2);
        bytes.extend(std::iter::repeat_n(LIST_BEGIN[0], DEPTH));
        bytes.extend(std::iter::repeat_n(LIST_END[0], DEPTH));

        let result: Result<destream::IgnoredAny, _> = decode_from_chunks(&bytes, 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncated_streams_fail() {
        let value = (HashMap::<String, u64>::from_iter([
            ("a".into(), 1),
            ("b".into(), 2),
        ]),);
        let bytes = encode_to_vec(value.clone()).await;

        // Determine the minimum prefix length required to decode successfully (TBON decoders are
        // allowed to stop after reading a single value).
        let mut min_success = None;
        for i in 0..=bytes.len() {
            let result: Result<(HashMap<String, u64>,), _> =
                decode_from_chunks(&bytes[..i], 1).await;
            if result.as_ref().is_ok_and(|decoded| decoded == &value) {
                min_success = Some(i);
                break;
            }
        }

        let min_success = min_success.expect("expected at least one successful decode");
        for i in 0..min_success {
            for chunk_size in [1usize, 2, 3, 7] {
                let result: Result<(HashMap<String, u64>,), _> =
                    decode_from_chunks(&bytes[..i], chunk_size).await;
                assert!(result.is_err(), "expected truncation to fail at {i}");
            }
        }
    }

    #[tokio::test]
    async fn test_corrupt_stream_fails() {
        let value = ("hello".to_string(), vec![1u8, 2, 3]);
        let mut bytes = encode_to_vec(value).await;
        if let Some(first) = bytes.first_mut() {
            *first = 0xFF;
        }

        let result: Result<(String, Vec<u8>), _> = decode_from_chunks(&bytes, 3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_default_impl_roundtrips() {
        run_test(()).await;

        run_test(Some("hello".to_string())).await;
        run_test::<Option<String>>(None).await;

        run_test(VecDeque::from([1u16, 2, 3, 4])).await;
        run_test(LinkedList::from([1i8, 2, 3, 4])).await;

        run_test(BTreeSet::from([1u8, 2, 3])).await;
        run_test(HashSet::from(["a".to_string(), "b".to_string()])).await;

        run_test(BTreeMap::from_iter([
            (1u64, "one".to_string()),
            (2u64, "two".to_string()),
        ]))
        .await;
        run_test(HashMap::<i32, String>::from_iter([
            (-1i32, "one".to_string()),
            (2i32, "two".to_string()),
        ]))
        .await;

        let array = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let array_ref: &[u8; 8] = &array;
        let encoded = encode(&array_ref).unwrap();
        let decoded: [u8; 8] = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, array);
        run_test((1u8, 2u16, 3u32, 4u64)).await;

        // BinaryHeap doesn't implement PartialEq; compare its sorted contents.
        let heap: BinaryHeap<i32> = BinaryHeap::from([3, 1, 2, 5, 4]);
        let encoded = encode(&heap).unwrap();
        let decoded: BinaryHeap<i32> = try_decode((), encoded).await.unwrap();
        assert_eq!(heap.clone().into_sorted_vec(), decoded.into_sorted_vec());

        // IgnoredAny must be able to consume any TBON value.
        let map: HashMap<String, Vec<u8>> =
            HashMap::from_iter([("a".to_string(), vec![1u8, 2, 3])]);
        let tuple = (map,);
        let encoded = encode(&tuple).unwrap();
        let _: destream::IgnoredAny = try_decode((), encoded).await.unwrap();

        run_test(i128::MAX).await;
        run_test(u128::MAX).await;
        run_test(NonZeroI128::new(-5_i128).unwrap()).await;
        run_test(NonZeroU128::new(5_u128).unwrap()).await;

        run_test(Duration::new(5, 7)).await;

        run_test(Ipv4Addr::new(127, 0, 0, 1)).await;
        run_test(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)).await;
        run_test(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await;
        run_test(SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), 80))).await;
    }

    #[tokio::test]
    async fn test_extended_default_impl_numeric_tokens() {
        let encoded = encode(123_i64).unwrap();
        let decoded: i128 = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, 123_i128);

        let encoded = encode(123_u64).unwrap();
        let decoded: u128 = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, 123_u128);

        let encoded = encode(i64::MAX).unwrap();
        let decoded: i128 = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, i64::MAX as i128);

        let encoded = encode(u64::MAX).unwrap();
        let decoded: u128 = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, u64::MAX as u128);
    }

    #[tokio::test]
    async fn test_extended_default_impl_decode_errors() {
        assert_decode_fails::<u128, _>(-1_i64).await;
        assert_decode_fails::<NonZeroU128, _>(0_u64).await;
        assert_decode_fails::<NonZeroI128, _>(0_i64).await;

        assert_decode_fails::<u128, _>(format!("{}0", u128::MAX)).await;
        assert_decode_fails::<i128, _>(format!("{}0", i128::MAX)).await;

        assert_decode_fails::<Duration, _>((5_u64,)).await;
        assert_decode_fails::<Duration, _>((5_u64, "7".to_string())).await;
        assert_decode_fails::<Duration, _>((5_u64, 1_000_000_000_u32)).await;

        assert_decode_fails::<Ipv4Addr, _>("999.0.0.1").await;
        assert_decode_fails::<Ipv6Addr, _>("not an ip").await;
        assert_decode_fails::<IpAddr, _>("not an ip").await;
        assert_decode_fails::<SocketAddr, _>("127.0.0.1").await;
    }

    #[tokio::test]
    async fn test_primitives() {
        run_test(true).await;
        run_test(false).await;

        for u in 0..66000u64 {
            run_test(u).await;
        }

        for i in -66000..66000i64 {
            run_test(i).await;
        }

        for _ in 0..100000 {
            let f: f32 = rand::rng().random();
            run_test(f).await;
        }
    }

    #[tokio::test]
    async fn test_undefined_numbers() {
        async fn recode<'en, T>(value: T) -> T
        where
            T: FromStream<Context = ()> + IntoStream<'en> + fmt::Debug + PartialEq + Clone + 'en,
        {
            let encoded = encode(value.clone()).unwrap();
            try_decode((), encoded).await.unwrap()
        }

        assert!(recode(f32::NAN).await.is_nan());

        let inf = recode(f32::INFINITY).await;
        assert!(inf.is_infinite() && inf.is_positive());

        let inf = recode(f32::NEG_INFINITY).await;
        assert!(inf.is_infinite() && inf.is_negative());

        assert!(recode(f64::NAN).await.is_nan());

        let inf = recode(f64::INFINITY).await;
        assert!(inf.is_infinite() && inf.is_sign_positive());

        let inf = recode(f64::NEG_INFINITY).await;
        assert!(inf.is_infinite() && inf.is_sign_negative());
    }

    #[tokio::test]
    async fn test_strings() {
        run_test(String::from("hello world")).await;
        run_test(String::from("Привет, мир")).await;
        run_test(String::from("this is a \"string\" within a \\ string")).await;
        run_test(String::from("this \"string\" is \\\terminated by a \\")).await;
    }

    #[tokio::test]
    async fn test_compound() {
        let list = vec![String::from("hello"), String::from("world")];
        run_test(list).await;

        let mut map = HashMap::new();
        map.insert(-1i32, String::from("I'm a teapot"));
        map.insert(-1i32, String::from("\' \"\"     "));
        run_test(map).await;

        let mut map = HashMap::new();
        map.insert("one".to_string(), HashMap::new());
        map.insert(
            "two".to_string(),
            HashMap::from_iter(vec![("three".to_string(), 4f32)]),
        );
        run_test(map).await;
    }

    #[tokio::test]
    async fn test_tuple() {
        let tuple: (Vec<u8>, HashMap<u64, String>) = (Vec::new(), HashMap::new());
        run_test(tuple).await;

        let tuple = (
            true,
            -1i16,
            3.14,
            String::from(" hello \"world\""),
            (0..255).collect::<Vec<u8>>(),
        );
        run_test(tuple).await;

        let tuple: (bool, Vec<String>, Option<String>, Vec<String>, bool) =
            (true, vec![], None, vec![], false);

        run_test(tuple).await;

        let tuple: (Bytes, BTreeMap<u64, String>) = (vec![1, 2, 3].into(), BTreeMap::new());
        run_test(tuple).await;

        let one = Bytes::from(vec![1, 2, 3]);
        let two = BTreeMap::new();
        let tuple: (Bytes, BTreeMap<u64, String>) = (one.clone(), two.clone());

        let encoded = encode(tuple).unwrap();
        let decoded: (Bytes, BTreeMap<u64, String>) = try_decode((), encoded).await.unwrap();
        assert_eq!(decoded, (one, two));
    }

    #[tokio::test]
    async fn test_array() {
        #[derive(Eq, PartialEq)]
        struct TestArray {
            data: Vec<i16>,
        }

        struct TestVisitor;

        impl destream::de::Visitor for TestVisitor {
            type Value = TestArray;

            fn expecting() -> &'static str {
                "a TestArray"
            }

            async fn visit_array_i16<A: destream::de::ArrayAccess<i16>>(
                self,
                mut array: A,
            ) -> Result<Self::Value, A::Error> {
                let mut data = Vec::with_capacity(3);
                let mut buffer = [0; 100];
                loop {
                    let num_items = array.buffer(&mut buffer).await?;
                    if num_items > 0 {
                        data.extend(&buffer[..num_items]);
                    } else {
                        break;
                    }
                }

                Ok(TestArray { data })
            }
        }

        impl FromStream for TestArray {
            type Context = ();

            async fn from_stream<D: destream::de::Decoder>(
                _: (),
                decoder: &mut D,
            ) -> Result<Self, D::Error> {
                decoder.decode_array_i16(TestVisitor).await
            }
        }

        impl<'en> destream::en::ToStream<'en> for TestArray {
            fn to_stream<E: destream::en::Encoder<'en>>(
                &'en self,
                encoder: E,
            ) -> Result<E::Ok, E::Error> {
                encoder.encode_array_i16(futures::stream::once(future::ready(
                    self.data.iter().copied(),
                )))
            }
        }

        impl fmt::Debug for TestArray {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                fmt::Debug::fmt(&self.data, f)
            }
        }

        let test = TestArray {
            data: (0..512).collect(),
        };

        let mut encoded = encode(&test).unwrap();
        let mut buf = Vec::new();
        while let Some(chunk) = encoded.try_next().await.unwrap() {
            buf.extend_from_slice(&chunk);
        }

        let decoded: TestArray = try_decode((), encode(&test).unwrap()).await.unwrap();
        assert_eq!(test, decoded);
    }

    #[tokio::test]
    async fn test_bytes() {
        run_test(Bytes::from(vec![1, 2, 3])).await;
    }

    #[tokio::test]
    async fn test_uuid() {
        run_test(Uuid::from_bytes([0u8; 16])).await;
    }
}
