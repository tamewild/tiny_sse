use std::fmt::{Debug, Display, Formatter};
use std::str::Utf8Error;
use std::string::FromUtf8Error;

mod parser;
mod stream;

pub use stream::*;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct Event {
    /// The event's type if provided. Otherwise, empty.
    pub ty: String,
    /// The event's data if provided. Otherwise, empty.
    pub data: String,
    /// The event's ID if provided. Otherwise, empty.
    pub id: String,
    /// A reconnection time, in milliseconds. This must initially be an implementation-defined value, probably in the region of a few seconds.
    ///
    /// Source: https://html.spec.whatwg.org/multipage/server-sent-events.html#concept-event-stream-reconnection-time
    pub retry: Option<u64>
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

impl<E: Debug + Display> Display for Error<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:#?}")
    }
}

impl<E: Debug + Display> std::error::Error for Error<E> {

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
