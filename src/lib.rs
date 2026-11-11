#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

use std::time::Duration;

mod error;
mod handler;
mod request;
mod response;
mod server;

pub use async_net::AsyncToSocketAddrs;
pub use bytes::Bytes;
pub use error::Error;
pub use handler::Handler;
pub use server::Server;

/// The in-memory body of a request or response.
pub type Body = Bytes;

/// Largest number of headers accepted on one request.
pub const MAX_HEADERS: usize = 64;

/// Largest request line plus headers accepted, in bytes.
pub const MAX_HEAD_BYTES: usize = 16 * 1024;

/// Largest request body accepted, in bytes.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

/// How long a connection may take to deliver one request once it has started, or to accept one response, before it is dropped.
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long an open connection may wait for its next request to start before it is dropped.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
