use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use destream::en::{Error as _, Event, IntoStream};
use futures::{ready, Stream};

use super::{ByteStream, Encoder};
use crate::constants::*;

struct Frame {
    map: bool,
    value: bool,
}

struct Events<'en, S> {
    source: Option<Pin<Box<S>>>,
    current: Option<ByteStream<'en>>,
    frames: Vec<Frame>,
    root: bool,
}

impl<'en, S> Events<'en, S> {
    fn fail(&mut self, cause: super::Error) -> Poll<Option<Result<Bytes, super::Error>>> {
        self.current = None;
        self.source = None;
        self.frames = Vec::new();

        Poll::Ready(Some(Err(cause)))
    }

    // Construct leaf streams before registering them to preserve error precedence.
    fn begin_value(&mut self) -> Result<(), super::Error> {
        if let Some(frame) = self.frames.last_mut() {
            if frame.map {
                frame.value = !frame.value;
            }

            Ok(())
        } else if self.root {
            Err(super::Error::custom("multiple root values in event stream"))
        } else {
            self.root = true;
            Ok(())
        }
    }

    fn end(&mut self) -> Result<bool, super::Error> {
        let frame = self
            .frames
            .pop()
            .ok_or_else(|| super::Error::custom("unexpected container end"))?;

        if frame.map && frame.value {
            return Err(super::Error::custom("map key is missing its value"));
        }

        Ok(frame.map)
    }

    fn finish(&self) -> Result<(), super::Error> {
        if !self.frames.is_empty() || !self.root {
            Err(super::Error::custom("incomplete event stream"))
        } else {
            Ok(())
        }
    }
}

impl<'en, T, S> Stream for Events<'en, S>
where
    T: IntoStream<'en> + 'en,
    S: Stream<Item = Result<Event<T>, super::Error>>,
{
    type Item = Result<Bytes, super::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(current) = &mut this.current {
                match ready!(current.as_mut().poll_next(cx)) {
                    Some(Ok(bytes)) => return Poll::Ready(Some(Ok(bytes))),
                    Some(Err(cause)) => return this.fail(cause),
                    None => this.current = None,
                }
            }

            let Some(source) = &mut this.source else {
                return Poll::Ready(None);
            };
            let event = match ready!(source.as_mut().poll_next(cx)) {
                Some(Ok(event)) => event,
                Some(Err(cause)) => return this.fail(cause),
                None => {
                    this.source = None;
                    return match this.finish() {
                        Ok(()) => Poll::Ready(None),
                        Err(cause) => this.fail(cause),
                    };
                }
            };

            match event {
                Event::End => {
                    let map = match this.end() {
                        Ok(map) => map,
                        Err(cause) => return this.fail(cause),
                    };

                    let end = if map { MAP_END } else { LIST_END };
                    return Poll::Ready(Some(Ok(Bytes::from_static(end))));
                }
                Event::SeqStart(_) | Event::MapStart(_) => {
                    let map = matches!(event, Event::MapStart(_));
                    if let Err(cause) = this.begin_value() {
                        return this.fail(cause);
                    }

                    this.frames.push(Frame { map, value: false });
                    let begin = if map { MAP_BEGIN } else { LIST_BEGIN };
                    return Poll::Ready(Some(Ok(Bytes::from_static(begin))));
                }
                Event::Value(value) => {
                    let current = match value.into_stream(Encoder) {
                        Ok(current) => current,
                        Err(cause) => return this.fail(cause),
                    };

                    if let Err(cause) = this.begin_value() {
                        return this.fail(cause);
                    }
                    this.current = Some(current);
                }
            }
        }
    }
}

pub(super) fn encode<'en, T, S>(events: S) -> ByteStream<'en>
where
    T: IntoStream<'en> + 'en,
    S: Stream<Item = Result<Event<T>, super::Error>> + Send + 'en,
{
    Box::pin(Events {
        source: Some(Box::pin(events)),
        current: None,
        frames: Vec::new(),
        root: false,
    })
}
