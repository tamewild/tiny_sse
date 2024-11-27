use std::str::Utf8Error;
use std::string::FromUtf8Error;

mod parser;
mod stream;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct Event {
    /// The event's type if provided. Otherwise, empty.
    ty: String,
    /// The event's data if provided. Otherwise, empty.
    data: String,
    /// The event's ID if provided. Otherwise, empty.
    id: String,
    /// A reconnection time, in milliseconds. This must initially be an implementation-defined value, probably in the region of a few seconds.
    ///
    /// Source: https://html.spec.whatwg.org/multipage/server-sent-events.html#concept-event-stream-reconnection-time
    retry: Option<u64>
}

#[derive(Debug, PartialEq)]
pub enum Error<E> {
    /// Failed to decode a UTF-8 string from the source stream
    StringUtf8(FromUtf8Error),
    /// Failed to decode a UTF-8 slice from the source stream
    StrUtf8(Utf8Error),
    /// Error originating from the source stream
    Source(E)
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
