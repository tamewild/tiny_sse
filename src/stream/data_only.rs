use std::pin::Pin;
use std::task::{ready, Context, Poll};
use futures_core::Stream;
use pin_project_lite::pin_project;
use crate::parser;
use crate::parser::{Field, FieldKind, ParseResult, ParsedLine};

// NOTE: This doesn't support multiple data lines in a single event.
// The last data line of the event will be used. The rest will be discarded.
fn parse_lines_for_data(mut buffer: &[u8]) -> (Option<&[u8]>, &[u8]) {
    let mut data = None;

    loop {
        let res = parser::parse_line(buffer);

        match res {
            ParseResult::Parsed { line, rem } => {
                buffer = rem;

                match line {
                    ParsedLine::Field(Field { kind: FieldKind::Data, value }) => {
                        data = Some(value);
                    }
                    ParsedLine::Dispatch => {
                        if let Some(data) = data {
                            break (Some(data), buffer)
                        }
                    }
                    _ => {}
                }
            }
            ParseResult::Incomplete => break (None, buffer)
        }
    }
}

pin_project! {
    pub struct DataOnlyStream<St, F> {
        #[pin]
        stream: St,
        f: F,
        buffer: Vec<u8>
    }
}

impl<St, F> DataOnlyStream<St, F> {
    pub fn new(stream: St, f: F) -> Self {
        Self {
            stream,
            f,
            buffer: Vec::new(),
        }
    }
}

impl<St, F, B, E, T> Stream for DataOnlyStream<St, F>
where
    St: Stream<Item = Result<B, E>>,
    F: FnMut(&[u8]) -> Result<T, E>,
    B: AsRef<[u8]>
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let (Some(data), rem) = parse_lines_for_data(&this.buffer) {
            let data = (this.f)(data);
            this.buffer.drain(..this.buffer.len() - rem.len());
            return Poll::Ready(Some(data))
        }

        Poll::Ready(loop {
            let Some(bytes) = ready!(this.stream.as_mut().poll_next(cx)) else {
                break None
            };

            let bytes = bytes?;

            let data = if this.buffer.is_empty() {
                let (data, rem) = parse_lines_for_data(bytes.as_ref());

                this.buffer.extend_from_slice(rem);

                data.map(&mut this.f)
            } else {
                this.buffer.extend_from_slice(bytes.as_ref());

                let (data, rem) = parse_lines_for_data(&this.buffer);

                let data = data.map(&mut this.f);

                this.buffer.drain(..this.buffer.len() - rem.len());

                data
            };

            match data {
                None => continue,
                Some(data) => break Some(data)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::pin::pin;
    use std::string::FromUtf8Error;
    use futures::{stream, StreamExt, TryStreamExt};
    use crate::EventStream;
    use crate::stream::data_only::DataOnlyStream;

    fn as_string(bytes: &[u8]) -> Result<String, FromUtf8Error> {
        String::from_utf8(bytes.to_vec())
    }

    #[tokio::test]
    async fn basic() {
        let body = "data: test\r\n\rdata: second event\n\r\ndata: third";

        let stream = DataOnlyStream::new(
            stream::once(async move {
                Ok(body)
            }),
            as_string
        );

        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(chunks, [
            "test",
            "second event"
        ]);
    }

    #[tokio::test]
    async fn parity() {
        let regular_chunks = crate::stream::tests::chunks(
            "misc/regular_chunks.json"
        ).await;
        let irregular_chunks = crate::stream::tests::chunks(
            "misc/irregular_chunks.json"
        ).await;

        let regular_data_chunks = {
            let mut stream = pin!(DataOnlyStream::new(
                stream::iter(&regular_chunks).map(Ok),
                as_string
            ));

            let mut vec = Vec::with_capacity(regular_chunks.len());

            while let Some(Ok(data)) = stream.next().await {
                vec.push(data);
            }

            assert_eq!(stream.buffer.capacity(), 0);
            assert_eq!(regular_chunks.len(), vec.len());

            vec
        };

        let irregular_data_chunks = DataOnlyStream::new(
            stream::iter(irregular_chunks).map(Ok),
            as_string
        ).try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(regular_data_chunks, irregular_data_chunks);

        let regular_events = EventStream::new(
            stream::iter(&regular_chunks).map(Ok::<_, Infallible>)
        )
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|ev| ev.data)
            .collect::<Vec<_>>();

        assert_eq!(regular_data_chunks, regular_events);
    }
}