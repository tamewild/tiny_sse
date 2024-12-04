use std::pin::Pin;
use std::task::{ready, Context, Poll};
use futures_core::Stream;
use pin_project_lite::pin_project;
use crate::parser;
use crate::parser::{Field, FieldKind, ParseResult, ParsedLine};

fn parse_lines_for_data(mut buffer: &[u8]) -> (Option<&[u8]>, &[u8]) {
    loop {
        let res = parser::parse_line(buffer);

        match res {
            ParseResult::Parsed { line, rem } => {
                buffer = rem;

                if let ParsedLine::Field(Field { kind: FieldKind::Data, value }) = line {
                    break (Some(value), buffer)
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
    use futures::{stream, TryStreamExt};
    use crate::stream::data_only::DataOnlyStream;

    #[tokio::test]
    async fn basic() {
        let body = "data: test\r\n\rdata: second event\n\r\ndata: third";

        let stream = DataOnlyStream::new(
            stream::once(async move {
                Ok(body)
            }),
            |bytes: &[u8]| String::from_utf8(bytes.to_vec())
        );

        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(chunks, vec![
            "test",
            "second event"
        ]);
    }
}