//! Decode a Rust data structure from a TBON-encoded stream.

use std::fmt;
use std::marker::PhantomData;

use bytes::{BufMut, Bytes, BytesMut};
use destream::{de, FromStream, Visitor};
use futures::stream::{Fuse, FusedStream, Stream, StreamExt, TryStreamExt};
use num_traits::{FromPrimitive, ToPrimitive};

#[cfg(feature = "tokio-io")]
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use super::constants::*;
use super::Element;

const CHUNK_SIZE: usize = 4096;
const SNIPPET_LEN: usize = 10;
const DEFAULT_MAX_DEPTH: usize = 1024;

/// Methods common to any decodable [`Stream`]
pub trait Read: Send + Unpin {
    /// Read the next chunk of [`Bytes`] in the [`Stream`], if any.
    async fn next(&mut self) -> Option<Result<Bytes, Error>>;

    /// Return `true` if there is no more content to be read from the [`Stream`].
    fn is_terminated(&self) -> bool;
}

/// A [`Stream`] to decode
pub struct SourceStream<S> {
    source: Fuse<S>,
}

impl<S: Stream<Item = Result<Bytes, Error>> + Send + Unpin> Read for SourceStream<S> {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        self.source.next().await
    }

    fn is_terminated(&self) -> bool {
        self.source.is_terminated()
    }
}

impl<S: Stream> From<S> for SourceStream<S> {
    fn from(source: S) -> Self {
        Self {
            source: source.fuse(),
        }
    }
}

/// A buffered reader of a decodable stream
#[cfg(feature = "tokio-io")]
pub struct SourceReader<R: AsyncRead> {
    reader: BufReader<R>,
    terminated: bool,
    scratch: BytesMut,
}

#[cfg(feature = "tokio-io")]
impl<R: AsyncRead + Send + Unpin> Read for SourceReader<R> {
    async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        self.scratch.clear();
        match self.reader.read_buf(&mut self.scratch).await {
            Ok(0) => {
                self.terminated = true;
                None
            }
            Ok(size) => {
                debug_assert_eq!(self.scratch.len(), size);
                Some(Ok(self.scratch.split().freeze()))
            }
            Err(cause) => Some(Err(de::Error::custom(format!("io error: {}", cause)))),
        }
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(feature = "tokio-io")]
impl<R: AsyncRead> From<R> for SourceReader<R> {
    fn from(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            terminated: false,
            scratch: BytesMut::new(),
        }
    }
}

/// An error encountered while decoding a TBON stream.
pub struct Error {
    message: String,
}

impl Error {
    fn invalid_utf8<I: fmt::Display>(info: I) -> Self {
        de::Error::custom(format!("invalid UTF-8: {}", info))
    }

    fn unexpected_end() -> Self {
        de::Error::custom("unexpected end of stream")
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.message, f)
    }
}

struct ArrayAccess<'a, S, T> {
    decoder: &'a mut Decoder<S>,
    dtype: PhantomData<T>,
    done: bool,
}

impl<'a, S: Read + 'a, T: Element> ArrayAccess<'a, S, T> {
    async fn new(decoder: &'a mut Decoder<S>) -> Result<ArrayAccess<'a, S, T>, Error> {
        let dtype = &[T::dtype().to_u8().unwrap()];

        decoder.expect_delimiter(ARRAY_DELIMIT).await?;
        decoder.expect_delimiter(dtype).await?;

        let done = decoder.maybe_delimiter(ARRAY_DELIMIT).await?;

        Ok(ArrayAccess {
            decoder,
            dtype: PhantomData,
            done,
        })
    }
}

impl<'a, S: Read + 'a, T: Element + Send> de::ArrayAccess<T> for ArrayAccess<'a, S, T> {
    type Error = Error;

    async fn buffer(&mut self, buffer: &mut [T]) -> Result<usize, Self::Error> {
        if self.done {
            return Ok(0);
        }

        let size = T::SIZE;
        let mut limit = buffer.len() * size;

        let mut i = 0;
        let mut escaped = false;

        while i < limit {
            while i >= self.decoder.remaining() && !self.decoder.source.is_terminated() {
                self.decoder.buffer().await?;
            }

            if i < self.decoder.remaining()
                && &self.decoder.available()[i..i + 1] == ARRAY_DELIMIT
                && !escaped
            {
                break;
            }

            if escaped {
                escaped = false;
            } else if self.decoder.available()[i] == ESCAPE[0] {
                escaped = true;
                limit += 1;
            }

            i += 1;
        }

        let mut escape = false;
        let mut escaped = BytesMut::with_capacity(i);
        for byte in &self.decoder.available()[..i] {
            let as_slice = std::slice::from_ref(byte);

            if escape {
                escaped.put_u8(*byte);
                escape = false;
            } else if as_slice == ESCAPE {
                escape = true;
            } else {
                escaped.put_u8(*byte);
            }
        }
        self.decoder.consume(i);

        let mut elements = 0;

        for bytes in escaped.chunks(size) {
            buffer[elements] = T::parse(bytes)?;
            elements += 1;
        }

        while self.decoder.remaining() == 0 {
            if self.decoder.source.is_terminated() {
                return Err(Error::unexpected_end());
            } else {
                self.decoder.buffer().await?;
            }
        }

        if self.decoder.available().starts_with(ARRAY_DELIMIT) {
            self.done = true;
            self.decoder.consume(1);
        }

        Ok(elements)
    }
}

struct MapAccess<'a, S> {
    decoder: &'a mut Decoder<S>,
    size_hint: Option<usize>,
    done: bool,
}

impl<'a, S: Read + 'a> MapAccess<'a, S> {
    async fn new(
        decoder: &'a mut Decoder<S>,
        size_hint: Option<usize>,
    ) -> Result<MapAccess<'a, S>, Error> {
        decoder.expect_delimiter(MAP_BEGIN).await?;
        decoder.push_depth()?;

        let done = decoder.maybe_delimiter(MAP_END).await?;
        if done {
            decoder.pop_depth();
        }

        Ok(MapAccess {
            decoder,
            size_hint,
            done,
        })
    }
}

impl<'a, S: Read + 'a> de::MapAccess for MapAccess<'a, S> {
    type Error = Error;

    async fn next_key<K: FromStream>(&mut self, context: K::Context) -> Result<Option<K>, Error> {
        if self.done {
            return Ok(None);
        }

        let key = K::from_stream(context, self.decoder).await?;

        Ok(Some(key))
    }

    async fn next_value<V: FromStream>(&mut self, context: V::Context) -> Result<V, Error> {
        if self.done {
            return Err(de::Error::custom(
                "called MapAccess::next_value but the map has already ended",
            ));
        }

        let value = V::from_stream(context, self.decoder).await?;

        if self.decoder.maybe_delimiter(MAP_END).await? {
            self.done = true;
            self.decoder.pop_depth();
        }

        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        self.size_hint
    }
}

struct SeqAccess<'a, S> {
    decoder: &'a mut Decoder<S>,
    size_hint: Option<usize>,
    done: bool,
}

impl<'a, S: Read + 'a> SeqAccess<'a, S> {
    async fn new(
        decoder: &'a mut Decoder<S>,
        size_hint: Option<usize>,
    ) -> Result<SeqAccess<'a, S>, Error> {
        decoder.expect_delimiter(LIST_BEGIN).await?;
        decoder.push_depth()?;

        let done = decoder.maybe_delimiter(LIST_END).await?;
        if done {
            decoder.pop_depth();
        }

        Ok(SeqAccess {
            decoder,
            size_hint,
            done,
        })
    }
}

impl<'a, S: Read + 'a> de::SeqAccess for SeqAccess<'a, S> {
    type Error = Error;

    async fn next_element<T: FromStream>(
        &mut self,
        context: T::Context,
    ) -> Result<Option<T>, Self::Error> {
        if self.done {
            return Ok(None);
        }

        let value = T::from_stream(context, self.decoder).await?;

        if self.decoder.maybe_delimiter(LIST_END).await? {
            self.done = true;
            self.decoder.pop_depth();
        }

        Ok(Some(value))
    }

    fn size_hint(&self) -> Option<usize> {
        self.size_hint
    }
}

/// A structure that decodes Rust values from a TBON stream.
pub struct Decoder<R> {
    source: R,
    buffer: Vec<u8>,
    offset: usize,
    max_depth: usize,
    depth: usize,
}

impl<R> Decoder<R> {
    fn with_max_depth(source: R, max_depth: usize) -> Self {
        Self {
            source,
            buffer: Vec::new(),
            offset: 0,
            max_depth,
            depth: 0,
        }
    }

    fn push_depth(&mut self) -> Result<(), Error> {
        if self.depth >= self.max_depth {
            return Err(de::Error::custom(format!(
                "nesting depth limit exceeded (max {})",
                self.max_depth
            )));
        }

        self.depth += 1;
        Ok(())
    }

    fn pop_depth(&mut self) {
        debug_assert!(self.depth > 0);
        self.depth -= 1;
    }

    fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.offset)
    }

    fn available(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }

    fn maybe_compact(&mut self) {
        if self.offset == 0 {
            return;
        }

        if self.offset == self.buffer.len() {
            self.buffer.clear();
            self.offset = 0;
        } else if self.offset > 8192 && self.offset > self.buffer.len() / 2 {
            self.buffer.drain(..self.offset);
            self.offset = 0;
        }
    }

    fn consume(&mut self, n: usize) {
        debug_assert!(self.remaining() >= n);
        self.offset += n;
        self.maybe_compact();
    }

    fn contents(&self, max_len: usize) -> String {
        let buf = self.available();
        let len = Ord::min(buf.len(), max_len);
        let mut chunks: Vec<String> = Vec::with_capacity(len);
        let mut chunk = Vec::with_capacity(len);
        let mut is_ascii = false;
        for c in &buf[..len] {
            if is_ascii != c.is_ascii() {
                chunks.push(chunk.iter().collect());
                chunk.clear();
                is_ascii = c.is_ascii();
            }

            if c.is_ascii() {
                chunk.push(*c as char);
            } else {
                chunk.extend(format!(" {} ", c).as_bytes().iter().map(|c| *c as char))
            }
        }

        if !chunk.is_empty() {
            chunks.push(chunk.into_iter().collect());
        }

        chunks.join("")
    }
}

#[cfg(feature = "tokio-io")]
impl<A: AsyncRead> Decoder<A>
where
    SourceReader<A>: Read,
{
    pub fn from_reader(reader: A) -> Decoder<SourceReader<A>> {
        Decoder::with_max_depth(SourceReader::from(reader), DEFAULT_MAX_DEPTH)
    }
}

impl<S: Stream> Decoder<SourceStream<S>>
where
    SourceStream<S>: Read,
{
    /// Create a new [`Decoder`] from a source [`Stream`].
    pub fn from_stream(stream: S) -> Decoder<SourceStream<S>> {
        Decoder::with_max_depth(SourceStream::from(stream), DEFAULT_MAX_DEPTH)
    }
}

impl<R: Read> Decoder<R> {
    async fn ensure_eof(&mut self) -> Result<(), Error> {
        if self.remaining() != 0 {
            return Err(de::Error::custom(
                "expected end of stream, found trailing bytes",
            ));
        }

        while !self.source.is_terminated() {
            self.buffer().await?;
            if self.remaining() != 0 {
                return Err(de::Error::custom(
                    "expected end of stream, found trailing bytes",
                ));
            }
        }

        Ok(())
    }

    async fn buffer(&mut self) -> Result<(), Error> {
        if let Some(data) = self.source.next().await {
            self.maybe_compact();
            self.buffer.extend(data?);
        }

        Ok(())
    }

    async fn buffer_string(
        &mut self,
        begin: &'static [u8],
        end: &'static [u8],
    ) -> Result<Bytes, Error> {
        self.expect_delimiter(begin).await?;

        let mut i = 0;
        let mut escaped = false;
        loop {
            while i >= self.remaining() && !self.source.is_terminated() {
                self.buffer().await?;
            }

            match self.available().get(i) {
                Some(b) if std::slice::from_ref(b) == end && !escaped => break,
                Some(_) => {}
                None if self.source.is_terminated() => return Err(Error::unexpected_end()),
                None => continue,
            }

            if escaped {
                escaped = false;
            } else if self.available()[i] == ESCAPE[0] {
                escaped = true;
            }

            i += 1;
        }

        let mut escape = false;
        let mut s = BytesMut::with_capacity(i);
        for byte in &self.available()[..i] {
            let as_slice = std::slice::from_ref(byte);

            if escape {
                s.put_u8(*byte);
                escape = false;
            } else if as_slice == ESCAPE {
                escape = true;
            } else {
                s.put_u8(*byte);
            }
        }

        self.consume(i + 1); // include end delimiter
        Ok(s.into())
    }

    async fn ignore_string(
        &mut self,
        begin: &'static [u8],
        end: &'static [u8],
    ) -> Result<(), Error> {
        self.expect_delimiter(begin).await?;

        let mut i = 0;
        let mut escaped = false;
        loop {
            while i >= self.remaining() && !self.source.is_terminated() {
                self.buffer().await?;
            }

            match self.available().get(i) {
                Some(b) if std::slice::from_ref(b) == end && !escaped => {
                    self.consume(i);
                    break;
                }
                Some(_) => {}
                None if self.source.is_terminated() => return Err(Error::unexpected_end()),
                None => continue,
            }

            if escaped {
                escaped = false;
            } else if self.available()[i] == ESCAPE[0] {
                escaped = true;
            }

            if i > CHUNK_SIZE {
                self.consume(i);
                i = 0;
            } else {
                i += 1;
            }
        }

        self.consume(1); // process the end delimiter
        Ok(())
    }

    async fn expect_delimiter(&mut self, delimiter: &[u8]) -> Result<(), Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        }

        if self.available().starts_with(delimiter) {
            self.consume(delimiter.len());
            Ok(())
        } else {
            fn char_to_string(c: u8) -> String {
                if c < b' ' {
                    c.to_string()
                } else {
                    (c as char).to_string()
                }
            }

            let actual = char_to_string(self.available()[0]);
            let expected = char_to_string(delimiter[0]);

            let snippet = self.contents(SNIPPET_LEN);
            Err(de::Error::custom(format!(
                "unexpected delimiter {}, expected {} at {}",
                actual, expected, snippet
            )))
        }
    }

    async fn ignore_value(&mut self) -> Result<(), Error> {
        enum Frame {
            List,
            Map { expecting_value: bool },
        }

        async fn fill<R: Read>(decoder: &mut Decoder<R>) -> Result<(), Error> {
            while decoder.remaining() == 0 && !decoder.source.is_terminated() {
                decoder.buffer().await?;
            }

            Ok(())
        }

        async fn ignore_array<R: Read>(decoder: &mut Decoder<R>) -> Result<(), Error> {
            decoder.expect_delimiter(ARRAY_DELIMIT).await?;
            fill(decoder).await?;
            if decoder.remaining() == 0 {
                return Err(Error::unexpected_end());
            }
            decoder.consume(1); // consume dtype byte

            let mut i = 0usize;
            let mut escaped = false;
            loop {
                while i >= decoder.remaining() && !decoder.source.is_terminated() {
                    decoder.buffer().await?;
                }

                match decoder.available().get(i) {
                    Some(b) if std::slice::from_ref(b) == ARRAY_DELIMIT && !escaped => {
                        decoder.consume(i + 1); // include end delimiter
                        return Ok(());
                    }
                    Some(b) if *b == ESCAPE[0] && !escaped => {
                        escaped = true;
                    }
                    Some(_) => {
                        escaped = false;
                    }
                    None if decoder.source.is_terminated() => return Err(Error::unexpected_end()),
                    None => continue,
                }

                i += 1;
            }
        }

        let mut stack: Vec<Frame> = Vec::new();
        let mut need_value = true;

        loop {
            if need_value {
                fill(self).await?;
                if self.remaining() == 0 {
                    return Err(Error::unexpected_end());
                }

                match self.available()[0] {
                    b if b == LIST_BEGIN[0] => {
                        self.expect_delimiter(LIST_BEGIN).await?;
                        self.push_depth()?;
                        if self.maybe_delimiter(LIST_END).await? {
                            // empty list
                            self.pop_depth();
                        } else {
                            stack.push(Frame::List);
                            need_value = true;
                            continue;
                        }
                    }
                    b if b == MAP_BEGIN[0] => {
                        self.expect_delimiter(MAP_BEGIN).await?;
                        self.push_depth()?;
                        if self.maybe_delimiter(MAP_END).await? {
                            // empty map
                            self.pop_depth();
                        } else {
                            stack.push(Frame::Map {
                                expecting_value: false,
                            });
                            need_value = false;
                            continue;
                        }
                    }
                    b if b == ARRAY_DELIMIT[0] => {
                        ignore_array(self).await?;
                    }
                    b if b == STRING_DELIMIT[0] => {
                        self.ignore_string(STRING_DELIMIT, STRING_DELIMIT).await?;
                    }
                    dtype => match Type::from_u8(dtype)
                        .ok_or_else(|| de::Error::custom(format!("invalid type bit: {dtype}")))?
                    {
                        Type::None => self.parse_unit().await?,
                        Type::Bool => {
                            let _ = self.parse_element::<bool>().await?;
                        }
                        Type::F32 => {
                            let _ = self.parse_element::<f32>().await?;
                        }
                        Type::F64 => {
                            let _ = self.parse_element::<f64>().await?;
                        }
                        Type::I8 => {
                            let _ = self.parse_element::<i8>().await?;
                        }
                        Type::I16 => {
                            let _ = self.parse_element::<i16>().await?;
                        }
                        Type::I32 => {
                            let _ = self.parse_element::<i32>().await?;
                        }
                        Type::I64 => {
                            let _ = self.parse_element::<i64>().await?;
                        }
                        Type::U8 => {
                            let _ = self.parse_element::<u8>().await?;
                        }
                        Type::U16 => {
                            let _ = self.parse_element::<u16>().await?;
                        }
                        Type::U32 => {
                            let _ = self.parse_element::<u32>().await?;
                        }
                        Type::U64 => {
                            let _ = self.parse_element::<u64>().await?;
                        }
                    },
                }

                need_value = false;
            }

            match stack.last_mut() {
                None => return Ok(()),
                Some(Frame::List) => {
                    if self.maybe_delimiter(LIST_END).await? {
                        self.pop_depth();
                        stack.pop();
                        continue;
                    }

                    need_value = true;
                }
                Some(Frame::Map { expecting_value }) => {
                    if !*expecting_value {
                        if self.maybe_delimiter(MAP_END).await? {
                            self.pop_depth();
                            stack.pop();
                            continue;
                        }

                        // parse the next key
                        *expecting_value = true;
                        need_value = true;
                    } else {
                        // parse the next value
                        *expecting_value = false;
                        need_value = true;
                    }
                }
            }
        }
    }

    async fn maybe_delimiter(&mut self, delimiter: &'static [u8]) -> Result<bool, Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            Ok(false)
        } else if self.available().starts_with(delimiter) {
            self.consume(delimiter.len());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn parse_element<N: Element>(&mut self) -> Result<N, Error> {
        while self.remaining() <= N::SIZE && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() <= N::SIZE {
            return Err(de::Error::invalid_length(
                self.remaining(),
                std::any::type_name::<N>(),
            ));
        }

        let dtype = self.available()[0];
        self.consume(1);
        if Some(dtype) == N::dtype().to_u8() {
            // no-op
        } else if let Some(dtype) = Type::from_u8(dtype) {
            return Err(de::Error::invalid_type(dtype, N::dtype()));
        } else {
            return Err(de::Error::invalid_value(dtype, "a TBON type bit"));
        }

        let value = N::parse(&self.available()[..N::SIZE])?;
        self.consume(N::SIZE);
        Ok(value)
    }

    async fn parse_string(&mut self) -> Result<String, Error> {
        let s = self.buffer_string(STRING_DELIMIT, STRING_DELIMIT).await?;
        String::from_utf8(s.to_vec()).map_err(Error::invalid_utf8)
    }

    async fn parse_unit(&mut self) -> Result<(), Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        }

        let dtype = self.available()[0];
        self.consume(1);
        match dtype {
            byte if Some(byte) == Type::None.to_u8() => Ok(()),
            other => match Type::from_u8(other) {
                Some(dtype) => Err(de::Error::invalid_type(dtype, Type::None)),
                None => Err(de::Error::invalid_type("(unknown)", Type::None)),
            },
        }
    }
}

impl<R: Read> de::Decoder for Decoder<R> {
    type Error = Error;

    async fn decode_any<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        }

        fn type_from(bit: u8) -> Result<Type, Error> {
            Type::from_u8(bit)
                .ok_or_else(|| de::Error::custom(format!("invalid type bit: {}", bit)))
        }

        match self.available()[0] {
            b if b == ARRAY_DELIMIT[0] => {
                while self.remaining() < 2 && !self.source.is_terminated() {
                    self.buffer().await?;
                }

                match type_from(self.available()[1])? {
                    Type::Bool => self.decode_array_bool(visitor).await,
                    Type::F32 => self.decode_array_f32(visitor).await,
                    Type::F64 => self.decode_array_f64(visitor).await,
                    Type::I16 => self.decode_array_i16(visitor).await,
                    Type::I32 => self.decode_array_i32(visitor).await,
                    Type::I64 => self.decode_array_i64(visitor).await,
                    Type::U8 => self.decode_array_u8(visitor).await,
                    Type::U16 => self.decode_array_u16(visitor).await,
                    Type::U32 => self.decode_array_u32(visitor).await,
                    Type::U64 => self.decode_array_u64(visitor).await,
                    dtype => Err(de::Error::invalid_type(dtype, "a supported array type")),
                }
            }
            b if b == LIST_BEGIN[0] => self.decode_seq(visitor).await,
            b if b == MAP_BEGIN[0] => self.decode_map(visitor).await,
            b if b == STRING_DELIMIT[0] => self.decode_string(visitor).await,
            dtype => match type_from(dtype)? {
                Type::None => self.decode_unit(visitor).await,
                Type::Bool => self.decode_bool(visitor).await,
                Type::F32 => self.decode_f32(visitor).await,
                Type::F64 => self.decode_f64(visitor).await,
                Type::I8 => self.decode_i8(visitor).await,
                Type::I16 => self.decode_i16(visitor).await,
                Type::I32 => self.decode_i32(visitor).await,
                Type::I64 => self.decode_i64(visitor).await,
                Type::U8 => self.decode_u8(visitor).await,
                Type::U16 => self.decode_u16(visitor).await,
                Type::U32 => self.decode_u32(visitor).await,
                Type::U64 => self.decode_u64(visitor).await,
            },
        }
    }

    async fn decode_bool<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let b = self.parse_element().await?;
        visitor.visit_bool(b)
    }

    async fn decode_bytes<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.decode_array_u8(visitor).await
    }

    async fn decode_i8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_element().await?;
        visitor.visit_i8(i)
    }

    async fn decode_i16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_element().await?;
        visitor.visit_i16(i)
    }

    async fn decode_i32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_element().await?;
        visitor.visit_i32(i)
    }

    async fn decode_i64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let i = self.parse_element().await?;
        visitor.visit_i64(i)
    }

    async fn decode_u8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_element().await?;
        visitor.visit_u8(u)
    }

    async fn decode_u16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_element().await?;
        visitor.visit_u16(u)
    }

    async fn decode_u32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_element().await?;
        visitor.visit_u32(u)
    }

    async fn decode_u64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let u = self.parse_element().await?;
        visitor.visit_u64(u)
    }

    async fn decode_f32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let f = self.parse_element().await?;
        visitor.visit_f32(f)
    }

    async fn decode_f64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let f = self.parse_element().await?;
        visitor.visit_f64(f)
    }

    async fn decode_array_bool<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_bool(access).await
    }

    async fn decode_array_i8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_i8(access).await
    }

    async fn decode_array_i16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_i16(access).await
    }

    async fn decode_array_i32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_i32(access).await
    }

    async fn decode_array_i64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_i64(access).await
    }

    async fn decode_array_u8<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_u8(access).await
    }

    async fn decode_array_u16<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_u16(access).await
    }

    async fn decode_array_u32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_u32(access).await
    }

    async fn decode_array_u64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_u64(access).await
    }

    async fn decode_array_f32<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_f32(access).await
    }

    async fn decode_array_f64<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = ArrayAccess::new(self).await?;
        visitor.visit_array_f64(access).await
    }

    async fn decode_string<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.parse_string().await?;
        visitor.visit_string(s)
    }

    async fn decode_option<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        while self.remaining() == 0 && !self.source.is_terminated() {
            self.buffer().await?;
        }

        if self.remaining() == 0 {
            return Err(Error::unexpected_end());
        }

        if Some(self.available()[0]) == Type::None.to_u8() {
            self.consume(1);
            visitor.visit_none()
        } else {
            visitor.visit_some(self).await
        }
    }

    async fn decode_map<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = MapAccess::new(self, None).await?;
        visitor.visit_map(access).await
    }

    async fn decode_seq<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, None).await?;
        visitor.visit_seq(access).await
    }

    async fn decode_tuple<V: Visitor>(
        &mut self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let access = SeqAccess::new(self, Some(len)).await?;
        visitor.visit_seq(access).await
    }

    async fn decode_unit<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.parse_unit().await?;
        visitor.visit_unit()
    }

    async fn decode_uuid<V: Visitor>(&mut self, visitor: V) -> Result<V::Value, Self::Error> {
        self.decode_array_u8(visitor).await
    }

    async fn decode_ignored_any<V: Visitor>(
        &mut self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.ignore_value().await?;
        visitor.visit_unit()
    }
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn decode<S: Stream<Item = Bytes> + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    decode_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for lists/maps.
pub async fn decode_with_max_depth<S: Stream<Item = Bytes> + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: S,
    max_depth: usize,
) -> Result<T, Error> {
    let mut decoder = Decoder::with_max_depth(
        SourceStream::from(source.map(Result::<Bytes, Error>::Ok)),
        max_depth,
    );
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context.
pub async fn try_decode<
    E: fmt::Display,
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    T: FromStream,
>(
    context: T::Context,
    source: S,
) -> Result<T, Error> {
    try_decode_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for lists/maps.
pub async fn try_decode_with_max_depth<
    E: fmt::Display,
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    T: FromStream,
>(
    context: T::Context,
    source: S,
    max_depth: usize,
) -> Result<T, Error> {
    let mut decoder = Decoder::with_max_depth(
        SourceStream::from(source.map_err(|e| de::Error::custom(e))),
        max_depth,
    );
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context.
#[cfg(feature = "tokio-io")]
pub async fn read_from<R: AsyncReadExt + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: R,
) -> Result<T, Error> {
    read_from_with_max_depth(context, source, DEFAULT_MAX_DEPTH).await
}

/// Decode the given TBON-encoded stream of bytes into an instance of `T` using the given context,
/// enforcing a maximum nesting depth for lists/maps.
#[cfg(feature = "tokio-io")]
pub async fn read_from_with_max_depth<R: AsyncReadExt + Send + Unpin, T: FromStream>(
    context: T::Context,
    source: R,
    max_depth: usize,
) -> Result<T, Error> {
    let mut decoder = Decoder::with_max_depth(SourceReader::from(source), max_depth);
    let decoded = T::from_stream(context, &mut decoder).await?;
    decoder.ensure_eof().await?;
    Ok(decoded)
}
