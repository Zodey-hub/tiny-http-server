//! HTTP/1.1 response serialisation.

use std::io::Write as _;

use futures_lite::io::{AsyncWrite, AsyncWriteExt};
use http::{HeaderName, Response, StatusCode, header};

use crate::{Body, Error};

/// Headers the server derives itself, and therefore strips from handler responses.
///
/// Letting a handler set these would risk a framing mismatch between what we announce and what we actually write.
const RESERVED_HEADERS: [HeaderName; 3] = [
    header::CONTENT_LENGTH,
    header::CONNECTION,
    header::TRANSFER_ENCODING,
];

/// Rough size of a status line plus the framing headers, used to size the scratch buffer.
const FRAMING_OVERHEAD_BYTES: usize = 128;

/// Writes whole responses to a byte stream, one at a time.
///
/// Mirrors [`RequestReader`](crate::request::RequestReader): the scratch buffer is owned by the writer rather than by each call, so a keep-alive connection serialises every response after the first without allocating.
pub(crate) struct ResponseWriter<W> {
    writer: W,
    buffer: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> ResponseWriter<W> {
    /// Wrap a writable stream.
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
        }
    }

    /// Serialise `response` and write it in a single flush; `keep_alive` selects the `Connection` value.
    ///
    /// `Content-Length` and `Connection` are always written by this method, replacing any same-named headers the handler set.
    /// Without `include_body` only the head is written, with the `Content-Length` of the body, as the answer to a `HEAD` request.
    pub(crate) async fn write_response(
        &mut self,
        response: Response<Body>,
        keep_alive: bool,
        include_body: bool,
    ) -> Result<(), Error> {
        let (parts, body) = response.into_parts();
        let status = parts.status.as_str();
        let reason = parts.status.canonical_reason().unwrap_or("Unknown");
        let connection = if keep_alive { "keep-alive" } else { "close" };

        // `clear` keeps the capacity, so after the first response we usually do not allocate again
        self.buffer.clear();
        self.buffer.reserve(FRAMING_OVERHEAD_BYTES + body.len());

        write!(self.buffer, "HTTP/1.1 {status} {reason}\r\n").unwrap(); // writing to a Vec cannot fail
        for (name, value) in &parts.headers {
            if RESERVED_HEADERS.contains(name) {
                continue;
            }
            self.buffer.extend_from_slice(name.as_str().as_bytes());
            self.buffer.extend_from_slice(b": ");
            self.buffer.extend_from_slice(value.as_bytes());
            self.buffer.extend_from_slice(b"\r\n");
        }
        write!(self.buffer, "Content-Length: {}\r\n", body.len()).unwrap(); // writing to a Vec cannot fail
        write!(self.buffer, "Connection: {connection}\r\n").unwrap(); // writing to a Vec cannot fail
        self.buffer.extend_from_slice(b"\r\n");
        if include_body {
            self.buffer.extend_from_slice(&body);
        }

        self.writer.write_all(&self.buffer).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Shut down the write half of the connection.
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        self.writer.close().await?;
        Ok(())
    }
}

/// Build a bare response carrying `status` and no body.
///
/// Used for errors raised before a handler ever sees the request.
pub(crate) fn status_only(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::new());
    *response.status_mut() = status;
    response
}
