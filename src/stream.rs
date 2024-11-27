use crate::parser::{Field, FieldKind, ParseResult, ParsedLine};
use crate::Error;
use crate::{parser, Event};
use futures_core::Stream;
use memchr::memchr;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

struct EventBuilder {
    event: Event
}

impl EventBuilder {
    fn process_field<E>(&mut self, field: Field) -> Result<(), Error<E>> {
        match field.kind {
            FieldKind::Event => {
                self.event.ty = String::from_utf8(field.value.to_vec())
                    .map_err(Error::StringUtf8)?;
            }
            FieldKind::Data => {
                self.event.data.push_str(
                    std::str::from_utf8(field.value)
                        .map_err(Error::StrUtf8)?
                );
                self.event.data.push('\n');
            }
            FieldKind::Id => {
                // If the id has U+0000 NULL, it is ignored
                if memchr(0x00, field.value).is_none() {
                    self.event.id = String::from_utf8(field.value.to_vec())
                        .map_err(Error::StringUtf8)?;
                }
            }
            FieldKind::Retry => {
                let val = std::str::from_utf8(field.value)
                    .map_err(Error::StrUtf8)?;

                if let Ok(time) = val.parse::<u64>() {
                    self.event.retry = Some(time);
                }
            }
        }

        Ok(())
    }

    fn dispatch(&mut self) -> Option<Event> {
        let mut event = std::mem::take(&mut self.event);

        if event.data.is_empty() {
            return None
        }

        if matches!(event.data.as_bytes().last(), Some(b'\n')) {
            event.data.pop();
        }

        if event.ty.is_empty() {
            event.ty = "message".to_string();
        }

        Some(event)
    }
}

/// Attempts to parse the lines provided in a buffer for a complete event.
///
/// Might return an event, otherwise will return when it encounters an incomplete line.
/// Also returns the remaining buffer bytes.
fn parse_lines_for_event<'a, 'b, E>(
    mut buffer: &'a [u8],
    builder: &'b mut EventBuilder
) -> Result<(Option<Event>, &'a [u8]), Error<E>> {
    loop {
        let res = parser::parse_line(buffer);

        match res {
            ParseResult::Parsed { line, rem } => {
                buffer = rem;

                match line {
                    ParsedLine::Field(field) => {
                        builder.process_field(field)?;
                    }
                    ParsedLine::Dispatch => {
                        if let Some(event) = builder.dispatch() {
                            break Ok((
                                Some(event),
                                rem
                            ))
                        }
                    }
                    ParsedLine::Ignored => {}
                }
            }
            ParseResult::Incomplete => break Ok((
                None,
                buffer
            ))
        }
    }
}

fn strip_bom<'a, 'b>(bytes: &'a [u8], bom_checked: &'b mut bool) -> &'a [u8] {
    const UTF_8_BOM: &[u8] = b"\xEE\xBB\xBF";

    if !*bom_checked && bytes.len() >= UTF_8_BOM.len() {
        *bom_checked = true;
        bytes.strip_prefix(UTF_8_BOM).unwrap_or_else(|| bytes)
    } else {
        bytes
    }
}


pin_project! {
    pub struct EventStream<St> {
        #[pin]
        stream: St,
        buffer: Vec<u8>,
        builder: EventBuilder,
        bom_checked: bool
    }
}

impl<St> EventStream<St> {
    pub fn new(stream: St) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
            builder: EventBuilder {
                event: Event::default(),
            },
            bom_checked: false,
        }
    }
}

impl<St, B, E> Stream for EventStream<St>
where
    St: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>
{
    type Item = Result<Event, Error<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // The remaining buffer could still have complete events. We should process them.
        if let (Some(event), rem) = parse_lines_for_event(&this.buffer, &mut this.builder)? {
            this.buffer.drain(..this.buffer.len() - rem.len());
            return Poll::Ready(Some(Ok(event)))
        }

        // Otherwise, poll the stream for new bytes.
        // If the buffer is empty, process for an event. Return an event if found. Rest should be buffered.
        // (we should also check for a BOM here)
        // If the buffer is NOT empty, buffer all the new bytes. Then, process for an event.

        Poll::Ready(loop {
            let Some(bytes) = ready!(this.stream.as_mut().poll_next(cx)) else {
                // Stream ended
                break None
            };

            let bytes = bytes.map_err(Error::Source)?;

            let event = if this.buffer.is_empty() {
                let bytes = strip_bom(bytes.as_ref(), &mut this.bom_checked);

                let (event, rem) = parse_lines_for_event(bytes, &mut this.builder)?;

                this.buffer.extend_from_slice(rem);

                event
            } else {
                this.buffer.extend_from_slice(bytes.as_ref());

                let bytes = strip_bom(bytes.as_ref(), &mut this.bom_checked);

                let (event, rem) = parse_lines_for_event(bytes, &mut this.builder)?;

                this.buffer.drain(..this.buffer.len() - rem.len());

                event
            };

            match event {
                None => continue,
                Some(event) => break Some(Ok(event))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::stream::EventStream;
    use crate::Event;
    use futures::{stream, TryStreamExt};
    use std::convert::Infallible;

    async fn assert_events(body: &str, events: Vec<Event>) {
        let stream = EventStream::new(stream::once(async move {
            Ok::<_, Infallible>(body)
        }));

        let stream_events = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events, stream_events);
    }

    fn message_event() -> Event {
        Event {
            ty: "message".to_string(),
            ..Event::default()
        }
    }

    #[tokio::test]
    async fn four_blocks() {
        let body = r#": test stream

data: first event
id: 1

data:second event
id

data:  third event

"#;

        assert_events(body, vec![
            Event {
                data: "first event".to_string(),
                id: "1".to_string(),
                ..message_event()
            },
            Event {
                data: "second event".to_string(),
                ..message_event()
            },
            Event {
                data: " third event".to_string(),
                ..message_event()
            }
        ]).await
    }

    #[tokio::test]
    async fn three_blocks() {
        let body = r#": test stream

data: first event
id: 1

data:second event
id

data:  third event"#;

        assert_events(body, vec![
            Event {
                data: "first event".to_string(),
                id: "1".to_string(),
                ..message_event()
            },
            Event {
                data: "second event".to_string(),
                ..message_event()
            },
        ]).await
    }
}