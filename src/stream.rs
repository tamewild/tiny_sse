use crate::parser::{Field, FieldKind, ParseResult};
use crate::Error;
use crate::{parser, Event};
use futures_core::Stream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::task::{Context, Poll};
use memchr::memchr;

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

// todo handle bom
pin_project! {
    pub struct EventStream<St> {
        stream: St,
        buffer: Vec<u8>
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

        todo!()
    }
}