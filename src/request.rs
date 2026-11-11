//! Incremental HTTP/1.x request parsing.
//!
//! [`RequestReader`] owns the read half of a connection plus a reusable [`BytesMut`] buffer.
//! Because HTTP keep-alive lets several requests share one connection, the buffer deliberately outlives each request: bytes belonging to the *next* request may already have arrived while we were parsing the current one, and they must not be dropped.
//!
//! Parsing is zero-copy. Once a whole head is buffered it is frozen into [`Bytes`], and the URI, every header value and the body are refcounted windows onto those same bytes rather than copies of them.

use bytes::{Bytes, BytesMut};
use futures_lite::io::{AsyncRead, AsyncReadExt};
use http::request::Parts;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri, Version, header};

use crate::{Body, Error, MAX_BODY_BYTES, MAX_HEAD_BYTES, MAX_HEADERS};

/// How many bytes to ask the socket for per `read` call.
const READ_CHUNK_BYTES: usize = 8 * 1024;

/// Reads whole requests off a byte stream, one at a time.
pub(crate) struct RequestReader<R> {
    reader: R,
    buffer: BytesMut,
    scratch: Box<[u8]>,
}

impl<R: AsyncRead + Unpin> RequestReader<R> {
    /// Wrap a readable stream.
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::new(),
            scratch: vec![0; READ_CHUNK_BYTES].into_boxed_slice(),
        }
    }

    /// Wait until the next request starts, or return `false` when the peer closed the connection.
    pub(crate) async fn wait_for_request(&mut self) -> Result<bool, Error> {
        // bytes left over from the previous read already belong to the next request
        if !self.buffer.is_empty() {
            return Ok(true);
        }

        let received = self.fill().await?;

        Ok(received > 0)
    }

    /// Read the next request from the stream, or `None` when the peer closed the connection cleanly at a request boundary.
    ///
    /// Fails with [`Error::Io`] if the socket fails, [`Error::UnexpectedEof`] if the peer disappears mid-request, or a protocol error if the request is malformed or exceeds the size limits.
    pub(crate) async fn read_request(&mut self) -> Result<Option<Request<Body>>, Error> {
        let Some(head_len) = self.locate_head().await? else {
            return Ok(None);
        };

        let head = self.buffer.split_to(head_len).freeze();
        let parts = parse_head(&head)?;
        let content_length = content_length(&parts.headers)?;

        // we do not reserve, only alloc when real body is coming
        while self.buffer.len() < content_length {
            if self.fill().await? == 0 {
                return Err(Error::UnexpectedEof);
            }
        }

        let body = self.buffer.split_to(content_length).freeze();

        Ok(Some(Request::from_parts(parts, body)))
    }

    /// Read until a whole head is buffered and return its length, or `None` on a clean end-of-stream at a request boundary.
    async fn locate_head(&mut self) -> Result<Option<usize>, Error> {
        loop {
            if let Some(head_len) = complete_head_len(&self.buffer)? {
                if head_len > MAX_HEAD_BYTES {
                    return Err(Error::HeadTooLarge);
                }

                return Ok(Some(head_len));
            }

            if self.buffer.len() > MAX_HEAD_BYTES {
                return Err(Error::HeadTooLarge);
            }

            if self.fill().await? == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::UnexpectedEof)
                };
            }
        }
    }

    /// Append one chunk from the stream to the buffer and return how many bytes arrived, `0` at end-of-stream.
    async fn fill(&mut self) -> Result<usize, Error> {
        let received = self.reader.read(&mut self.scratch).await?;
        self.buffer.extend_from_slice(&self.scratch[..received]);
        Ok(received)
    }
}

/// Whether the connection should be reused after this request.
///
/// HTTP/1.1 keeps connections open unless the peer sends `Connection: close`; HTTP/1.0 closes them unless the peer opts in with `Connection: keep-alive`.
pub(crate) fn wants_keep_alive(request: &Request<Body>) -> bool {
    let connection = request
        .headers()
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    let has_token = |wanted: &str| {
        connection
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case(wanted))
    };

    if request.version() == Version::HTTP_10 {
        has_token("keep-alive")
    } else {
        !has_token("close")
    }
}

/// Parse a complete, frozen request head, which must be exactly the bytes [`complete_head_len`] measured.
fn parse_head(head: &Bytes) -> Result<Parts, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut parsed = httparse::Request::new(&mut headers);
    parsed.parse(head)?;

    let method = parsed.method.unwrap(); // a complete parse always sets the method
    let path = parsed.path.unwrap(); // a complete parse always sets the path
    let version_number = parsed.version.unwrap(); // a complete parse always sets the version
    let version = match version_number {
        0 => Version::HTTP_10,
        1 => Version::HTTP_11,
        _ => return Err(Error::UnsupportedVersion),
    };

    build_parts(head, method, path, version, parsed.headers).map_err(Error::InvalidField)
}

/// Assemble [`Parts`], sharing `head`'s allocation wherever `http` allows it.
fn build_parts(
    head: &Bytes,
    method: &str,
    path: &str,
    version: Version,
    parsed: &[httparse::Header<'_>],
) -> Result<Parts, http::Error> {
    let mut request = Request::new(());
    *request.version_mut() = version;
    *request.method_mut() = Method::try_from(method)?;
    *request.uri_mut() = Uri::from_maybe_shared(head.slice_ref(path.as_bytes()))?;

    let headers = request.headers_mut();
    headers.reserve(parsed.len());
    for header in parsed {
        let name = HeaderName::try_from(header.name)?;
        let value = HeaderValue::from_maybe_shared(head.slice_ref(header.value))?;
        headers.append(name, value);
    }

    // drop the placeholder body, the caller attaches the real one
    let (parts, ()) = request.into_parts();
    Ok(parts)
}

/// Determine how many body bytes follow the head.
///
/// Rejects chunked requests, a duplicated or unparseable `Content-Length`, and a length above [`MAX_BODY_BYTES`].
fn content_length(headers: &HeaderMap) -> Result<usize, Error> {
    if headers.contains_key(header::TRANSFER_ENCODING) {
        return Err(Error::UnsupportedTransferEncoding);
    }

    let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
    let Some(value) = values.next() else {
        return Ok(0);
    };

    // if multiple lengths are provided we treat it as error instead of trying to guess
    if values.next().is_some() {
        return Err(Error::InvalidContentLength);
    }

    let length: usize = value
        .to_str()
        .map_err(|_| Error::InvalidContentLength)?
        .trim()
        .parse()
        .map_err(|_| Error::InvalidContentLength)?;

    if length > MAX_BODY_BYTES {
        return Err(Error::BodyTooLarge);
    }

    Ok(length)
}

/// Find where a request head ends within `buffer`: its length in bytes, or `None` if `buffer` holds only part of one.
fn complete_head_len(buffer: &[u8]) -> Result<Option<usize>, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let status = httparse::Request::new(&mut headers).parse(buffer)?;

    match status {
        httparse::Status::Complete(head_len) => Ok(Some(head_len)),
        httparse::Status::Partial => Ok(None),
    }
}
