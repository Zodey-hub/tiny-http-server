//! Error types for the server.

use http::StatusCode;

/// Everything that can go wrong while serving a connection; it ends the connection, it never reaches the caller of the server.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// The underlying socket failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The peer closed the connection part-way through a request.
    #[error("connection closed mid-request")]
    UnexpectedEof,

    /// The request line or headers are not valid HTTP/1.x.
    #[error("malformed request head")]
    Parse(#[from] httparse::Error),

    /// The head parsed, but a field is not representable as a `http` type (e.g. an invalid method token or a non-ASCII header value).
    #[error("invalid request field")]
    InvalidField(#[from] http::Error),

    /// The request line and headers exceeded the limit of [`ServerBuilder::max_head_bytes`](crate::ServerBuilder::max_head_bytes).
    #[error("request head exceeds the limit")]
    HeadTooLarge,

    /// `Content-Length` exceeded the limit of [`ServerBuilder::max_body_bytes`](crate::ServerBuilder::max_body_bytes).
    #[error("request body exceeds the limit")]
    BodyTooLarge,

    /// `Content-Length` was absent-but-required, duplicated, or not a number.
    #[error("missing or malformed Content-Length")]
    InvalidContentLength,

    /// Only HTTP/1.0 and HTTP/1.1 are supported.
    #[error("unsupported HTTP version")]
    UnsupportedVersion,

    /// `Transfer-Encoding` (including `chunked`) is not implemented.
    #[error("Transfer-Encoding is not supported")]
    UnsupportedTransferEncoding,
}

impl Error {
    /// The status code to report to the peer, or `None` if the peer is unreachable.
    pub(crate) fn client_status(&self) -> Option<StatusCode> {
        match self {
            Error::Io(_) | Error::UnexpectedEof => None,
            Error::Parse(_)
            | Error::InvalidField(_)
            | Error::InvalidContentLength
            | Error::UnsupportedVersion => Some(StatusCode::BAD_REQUEST),
            Error::HeadTooLarge => Some(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE),
            Error::BodyTooLarge => Some(StatusCode::PAYLOAD_TOO_LARGE),
            Error::UnsupportedTransferEncoding => Some(StatusCode::NOT_IMPLEMENTED),
        }
    }
}
