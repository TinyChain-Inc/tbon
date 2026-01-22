//! Encode a Rust data structure into a TBON stream.

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::pin::Pin;

use bytes::{BufMut, Bytes, BytesMut};
use destream::{en, IntoStream};
use futures::future;
use futures::stream::{Stream, StreamExt};
use num_traits::ToPrimitive;
use uuid::Uuid;

use super::constants::*;
use super::element::{Element, IntoBytes};

mod stream;

/// A [`Stream`] of [`Bytes`] chunks
pub type ByteStream<'en> = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en>>;

/// An encoding error
pub struct Error {
    message: String,
}

impl en::Error for Error {
    fn custom<I: fmt::Display>(info: I) -> Self {
        Self {
            message: info.to_string(),
        }
    }
}

impl std::error::Error for Error {}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// An [`Encoder`] for a map of keys to values
pub struct MapEncoder<'en> {
    pending_key: Option<ByteStream<'en>>,
    entries: VecDeque<(ByteStream<'en>, ByteStream<'en>)>,
}

impl<'en> MapEncoder<'en> {
    #[inline]
    fn new(size_hint: Option<usize>) -> Self {
        let entries = if let Some(len) = size_hint {
            VecDeque::with_capacity(len)
        } else {
            VecDeque::new()
        };

        Self {
            pending_key: None,
            entries,
        }
    }
}

impl<'en> en::EncodeMap<'en> for MapEncoder<'en> {
    type Ok = ByteStream<'en>;
    type Error = Error;

    #[inline]
    fn encode_key<T: en::IntoStream<'en> + 'en>(&mut self, key: T) -> Result<(), Self::Error> {
        if self.pending_key.is_none() {
            self.pending_key = Some(key.into_stream(Encoder)?);
            Ok(())
        } else {
            Err(en::Error::custom(
                "You must call encode_value before calling encode_key again",
            ))
        }
    }

    #[inline]
    fn encode_value<T: en::IntoStream<'en> + 'en>(&mut self, value: T) -> Result<(), Self::Error> {
        if self.pending_key.is_none() {
            return Err(en::Error::custom(
                "You must call encode_key before encode_value",
            ));
        }

        let value = value.into_stream(Encoder)?;

        let mut key = None;
        mem::swap(&mut self.pending_key, &mut key);

        self.entries.push_back((key.expect("key"), value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if self.pending_key.is_some() {
            return Err(en::Error::custom(
                "You must call encode_value after calling encode_key",
            ));
        }
        Ok(stream::encode_map_encoded(self.entries))
    }
}

/// An [`Encoder`] for a sequence of values
pub struct SequenceEncoder<'en> {
    items: VecDeque<ByteStream<'en>>,
}

impl<'en> SequenceEncoder<'en> {
    #[inline]
    fn new(size_hint: Option<usize>) -> Self {
        let items = if let Some(len) = size_hint {
            VecDeque::with_capacity(len)
        } else {
            VecDeque::new()
        };

        Self { items }
    }

    #[inline]
    fn push(&mut self, value: ByteStream<'en>) {
        self.items.push_back(value);
    }

    fn encode(self) -> Result<ByteStream<'en>, Error> {
        Ok(stream::encode_list_encoded(self.items))
    }
}

impl<'en> en::EncodeSeq<'en> for SequenceEncoder<'en> {
    type Ok = ByteStream<'en>;
    type Error = Error;

    #[inline]
    fn encode_element<T: en::IntoStream<'en> + 'en>(
        &mut self,
        value: T,
    ) -> Result<(), Self::Error> {
        let encoded = value.into_stream(Encoder)?;
        self.push(encoded);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.encode()
    }
}

impl<'en> en::EncodeTuple<'en> for SequenceEncoder<'en> {
    type Ok = ByteStream<'en>;
    type Error = Error;

    #[inline]
    fn encode_element<T: en::IntoStream<'en> + 'en>(
        &mut self,
        value: T,
    ) -> Result<(), Self::Error> {
        let encoded = value.into_stream(Encoder)?;
        self.push(encoded);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.encode()
    }
}

/// A TBON encoder
pub struct Encoder;

impl Encoder {
    #[inline]
    fn encode_type<'en>(&self, dtype: &Type, value: &[u8]) -> Result<ByteStream<'en>, Error> {
        let mut chunk = BytesMut::with_capacity(value.len() + 1);
        chunk.put_u8(dtype.to_u8().expect("type bit"));
        chunk.extend_from_slice(value);

        Ok(Box::pin(futures::stream::once(future::ready(Ok(
            chunk.into()
        )))))
    }

    #[inline]
    fn encode_string_type<'en>(
        &self,
        start: u8,
        value: &[u8],
        end: u8,
    ) -> Result<ByteStream<'en>, Error> {
        let mut chunk = BytesMut::with_capacity(value.len() + 2);
        chunk.put_u8(start);
        chunk.extend(self.escape(value, &[start, end]));
        chunk.put_u8(end);

        Ok(Box::pin(futures::stream::once(future::ready(Ok(
            chunk.into()
        )))))
    }

    fn escape(&self, value: &[u8], control: &[u8]) -> Vec<u8> {
        let mut escaped = Vec::with_capacity(value.len() * 2);
        for char in value {
            if control.contains(char) || char == &ESCAPE[0] {
                escaped.push(ESCAPE[0])
            }

            escaped.push(*char);
        }

        escaped
    }
}

impl<'en> en::Encoder<'en> for Encoder {
    type Ok = ByteStream<'en>;
    type Error = Error;
    type EncodeMap = MapEncoder<'en>;
    type EncodeSeq = SequenceEncoder<'en>;
    type EncodeTuple = SequenceEncoder<'en>;

    #[inline]
    fn encode_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let value = if v { TRUE } else { FALSE };
        self.encode_type(&Type::Bool, value)
    }

    #[inline]
    fn encode_bytes<B: Into<Bytes>>(self, bytes: B) -> Result<Self::Ok, Self::Error> {
        self.collect_bytes(bytes.into())
    }

    #[inline]
    fn encode_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::I8, &v.to_be_bytes())
    }

    fn encode_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::I16, &v.to_be_bytes())
    }

    #[inline]
    fn encode_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::I32, &v.to_be_bytes())
    }

    #[inline]
    fn encode_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::I64, &v.to_be_bytes())
    }

    #[inline]
    fn encode_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::U8, &v.to_be_bytes())
    }

    #[inline]
    fn encode_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::U16, &v.to_be_bytes())
    }

    #[inline]
    fn encode_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::U32, &v.to_be_bytes())
    }

    #[inline]
    fn encode_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::U64, &v.to_be_bytes())
    }

    #[inline]
    fn encode_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::F32, &v.to_be_bytes())
    }

    #[inline]
    fn encode_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.encode_type(&Type::F64, &v.to_be_bytes())
    }

    fn encode_array_bool<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = bool> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::Bool, chunks))
    }

    fn encode_array_i8<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = i8> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::I8, chunks))
    }

    fn encode_array_i16<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = i16> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::I16, chunks))
    }

    fn encode_array_i32<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = i32> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::I32, chunks))
    }

    fn encode_array_i64<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = i64> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::I64, chunks))
    }

    fn encode_array_u8<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = u8> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::U8, chunks))
    }

    fn encode_array_u16<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = u16> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::U16, chunks))
    }

    fn encode_array_u32<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = u32> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::U32, chunks))
    }

    fn encode_array_u64<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = u64> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::U64, chunks))
    }

    fn encode_array_f32<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = f32> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::F32, chunks))
    }

    fn encode_array_f64<T, S>(self, chunks: S) -> Result<Self::Ok, Self::Error>
    where
        T: IntoIterator<Item = f64> + Send + Unpin + 'en,
        S: Stream<Item = T> + Send + Unpin + 'en,
        <T as IntoIterator>::IntoIter: Send + Unpin + 'en,
    {
        Ok(encode_array(Type::F64, chunks))
    }

    #[inline]
    fn encode_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.encode_string_type(STRING_DELIMIT[0], v.as_bytes(), STRING_DELIMIT[0])
    }

    #[inline]
    fn encode_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Box::pin(futures::stream::once(future::ready(Ok(
            Bytes::from(vec![Type::None.to_u8().expect("type bit")]),
        )))))
    }

    #[inline]
    fn encode_some<T: en::IntoStream<'en> + 'en>(self, value: T) -> Result<Self::Ok, Self::Error> {
        value.into_stream(self)
    }

    #[inline]
    fn encode_unit(self) -> Result<Self::Ok, Self::Error> {
        self.encode_none()
    }

    #[inline]
    fn encode_uuid(self, uuid: Uuid) -> Result<Self::Ok, Self::Error> {
        self.collect_bytes(uuid.as_bytes().iter().copied())
    }

    #[inline]
    fn encode_map(self, size_hint: Option<usize>) -> Result<Self::EncodeMap, Self::Error> {
        Ok(MapEncoder::new(size_hint))
    }

    #[inline]
    fn encode_map_stream<K, V, S>(self, map: S) -> Result<Self::Ok, Self::Error>
    where
        K: en::IntoStream<'en> + 'en,
        V: en::IntoStream<'en> + 'en,
        S: Stream<Item = (K, V)> + Send + Unpin + 'en,
    {
        Ok(Box::pin(stream::encode_map(map)))
    }

    #[inline]
    fn encode_seq(self, size_hint: Option<usize>) -> Result<Self::EncodeSeq, Self::Error> {
        Ok(SequenceEncoder::new(size_hint))
    }

    #[inline]
    fn encode_seq_stream<T: en::IntoStream<'en> + 'en, S: Stream<Item = T> + Send + Unpin + 'en>(
        self,
        seq: S,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Box::pin(stream::encode_list(seq)))
    }

    #[inline]
    fn encode_tuple(self, len: usize) -> Result<Self::EncodeTuple, Self::Error> {
        Ok(SequenceEncoder::new(Some(len)))
    }

    #[inline]
    fn collect_bytes<B: IntoIterator<Item = u8>>(self, bytes: B) -> Result<Self::Ok, Self::Error> {
        let bytes = bytes.into_iter();
        let mut array = match bytes.size_hint() {
            (0, None) | (0, Some(usize::MAX)) => Vec::new(),
            (_min, Some(max)) => Vec::with_capacity(max + 3),
            (min, None) => Vec::with_capacity(min),
        };

        array.extend_from_slice(ARRAY_DELIMIT);
        array.push(u8::dtype().to_u8().expect("type bit"));

        for byte in bytes {
            let as_slice = std::slice::from_ref(&byte);
            if as_slice == ARRAY_DELIMIT || as_slice == ESCAPE {
                array.extend_from_slice(ESCAPE);
            }

            array.put_u8(byte);
        }

        array.extend_from_slice(ARRAY_DELIMIT);

        let array: ByteStream = Box::pin(futures::stream::once(future::ready(Ok(array.into()))));
        Ok(array)
    }
}

#[inline]
fn delimiter<'en>(delimiter: &'static [u8]) -> ByteStream<'en> {
    Box::pin(futures::stream::once(future::ready(Ok(
        Bytes::from_static(delimiter),
    ))))
}

/// Given an encodable value, return an encoded stream.
pub fn encode<'en, T: IntoStream<'en> + 'en>(
    value: T,
) -> Result<impl Stream<Item = Result<Bytes, Error>> + 'en, Error> {
    value.into_stream(Encoder)
}

/// Given an encodable value, return an encoded stream buffered into chunks of `target` bytes.
///
/// This can reduce downstream overhead when the returned stream is consumed by an IO layer which
/// performs one `await`/write per chunk.
pub fn encode_buffered<'en, T: IntoStream<'en> + 'en>(
    value: T,
    target: usize,
) -> Result<impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en, Error> {
    Ok(stream::CoalesceStream::new(
        value.into_stream(Encoder)?,
        target,
    ))
}

/// Given a stream of encodable key-value pairs, return an encoded map stream.
pub fn encode_map<'en, K, V, S>(
    seq: S,
) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en
where
    K: IntoStream<'en> + 'en,
    V: IntoStream<'en> + 'en,
    S: Stream<Item = (K, V)> + Send + Unpin + 'en,
{
    stream::encode_map(seq)
}

/// Given a stream of encodable key-value pairs, return an encoded map stream buffered into chunks
/// of `target` bytes.
pub fn encode_map_buffered<'en, K, V, S>(
    seq: S,
    target: usize,
) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en
where
    K: IntoStream<'en> + 'en,
    V: IntoStream<'en> + 'en,
    S: Stream<Item = (K, V)> + Send + Unpin + 'en,
{
    stream::CoalesceStream::new(stream::encode_map(seq), target)
}

/// Given a stream of encodable elements, return an encoded sequence stream.
pub fn encode_seq<'en, T, S>(
    seq: S,
) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en
where
    T: IntoStream<'en> + 'en,
    S: Stream<Item = T> + Send + Unpin + 'en,
{
    stream::encode_list(seq)
}

/// Given a stream of encodable elements, return an encoded sequence stream buffered into chunks of
/// `target` bytes.
pub fn encode_seq_buffered<'en, T, S>(
    seq: S,
    target: usize,
) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'en
where
    T: IntoStream<'en> + 'en,
    S: Stream<Item = T> + Send + Unpin + 'en,
{
    stream::CoalesceStream::new(stream::encode_list(seq), target)
}

fn encode_array<'en, const SIZE: usize, E, T, S>(dtype: Type, chunks: S) -> ByteStream<'en>
where
    E: IntoBytes<SIZE>,
    T: IntoIterator<Item = E>,
    S: Stream<Item = T> + Send + Unpin + 'en,
{
    let mut start = BytesMut::with_capacity(2);
    start.extend_from_slice(ARRAY_DELIMIT);
    start.put_u8(dtype.to_u8().expect("type bit"));

    let start = futures::stream::once(future::ready(Ok(Bytes::from(start))));
    let end = delimiter(ARRAY_DELIMIT);

    let contents = chunks.map(|chunk| {
        let mut encoded = BytesMut::new();

        for b in chunk.into_iter() {
            for byte in b.into_bytes() {
                let as_slice = std::slice::from_ref(&byte);
                if as_slice == ARRAY_DELIMIT || as_slice == ESCAPE {
                    encoded.extend_from_slice(ESCAPE);
                }

                encoded.put_u8(byte);
            }
        }

        Ok(encoded.into())
    });

    let encoded: ByteStream = Box::pin(start.chain(contents).chain(end));
    encoded
}
