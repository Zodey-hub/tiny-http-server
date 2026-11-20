//! Connection acceptance and the per-connection request loop.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::thread::{self, Scope};
use std::time::Duration;

use async_channel::Receiver;
use async_executor::Executor;
use async_io::Timer;
use async_net::TcpListener;
use futures_lite::{AsyncRead, AsyncWrite, future};
use http::Method;

use crate::request::{RequestReader, wants_keep_alive};
use crate::response::{ResponseWriter, status_only};
use crate::{CONNECTION_TIMEOUT, Handler, IDLE_TIMEOUT, Limits};

/// An HTTP/1.1 server that dispatches every request to a single [`Handler`].
pub struct Server<H> {
    listener: TcpListener,
    handler: H,
    limits: Limits,
    threads: NonZeroUsize,
}

impl<H: Handler> Server<H> {
    /// Bind `addr` and create a server with the default settings of [`ServerBuilder`] that routes every request to `handler`.
    ///
    /// `addr` is any address [`ToSocketAddrs`] takes, such as `"127.0.0.1:8080"`.
    /// Binding port `0` lets the operating system pick a free port; [`Server::local_addr`] reports which one.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be resolved or bound.
    pub fn bind<A: ToSocketAddrs>(addr: A, handler: H) -> io::Result<Self> {
        ServerBuilder::new().bind(addr, handler)
    }

    /// The address the listener is bound to, including the port the operating system picked when port `0` was requested.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system cannot report the address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve connections until an accept error occurs, so it returns only an error.
    ///
    /// The current thread only accepts the connections, the threads of [`ServerBuilder::threads`] serve them on one shared executor, so a handler that blocks holds up only its own thread.
    ///
    /// # Errors
    ///
    /// Returns an error if a thread cannot be started or the listener stops accepting.
    pub fn run(&self) -> io::Result<Infallible> {
        let executor = Executor::new();
        let (stop_sender, stop_receiver) = async_channel::bounded::<()>(1);

        thread::scope(|scope| {
            let served = self.serve(scope, &executor, &stop_receiver);
            drop(stop_sender);
            served
        })
    }

    /// Start the worker threads of `executor` in `scope`, then accept connections on the current thread.
    fn serve<'scope, 'server: 'scope>(
        &'server self,
        scope: &'scope Scope<'scope, '_>,
        executor: &'scope Executor<'server>,
        stop: &Receiver<()>,
    ) -> io::Result<Infallible> {
        for _ in 0..self.threads.get() {
            let stop = stop.clone();
            thread::Builder::new().spawn_scoped(scope, move || {
                let _ = async_io::block_on(executor.run(stop.recv()));
            })?;
        }

        let accept_loop = accept_connections(&self.listener, &self.handler, self.limits, executor);
        async_io::block_on(accept_loop)
    }
}

/// Builds a [`Server`] with other limits or another number of threads than the defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerBuilder {
    limits: Limits,
    threads: NonZeroUsize,
}

impl ServerBuilder {
    /// Start from the defaults.
    pub fn new() -> Self {
        let threads = thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);

        Self {
            limits: Limits::default(),
            threads,
        }
    }

    /// Set the largest request line plus headers in bytes, [`DEFAULT_MAX_HEAD_BYTES`](crate::DEFAULT_MAX_HEAD_BYTES) by default; a larger head gets `431 Request Header Fields Too Large`.
    #[must_use]
    pub fn max_head_bytes(mut self, max_head_bytes: usize) -> Self {
        self.limits.max_head_bytes = max_head_bytes;
        self
    }

    /// Set the largest request body in bytes, [`DEFAULT_MAX_BODY_BYTES`](crate::DEFAULT_MAX_BODY_BYTES) by default; a larger `Content-Length` gets `413 Content Too Large` before the body is read.
    #[must_use]
    pub fn max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.limits.max_body_bytes = max_body_bytes;
        self
    }

    /// Set the number of threads that serve the connections, by default the number of CPUs ([`thread::available_parallelism`]), or one if it is unknown.
    #[must_use]
    pub fn threads(mut self, threads: NonZeroUsize) -> Self {
        self.threads = threads;
        self
    }

    /// Bind `addr` and create a server with these settings that routes every request to `handler`.
    ///
    /// `addr` is any address [`ToSocketAddrs`] takes, such as `"127.0.0.1:8080"`.
    /// Binding port `0` lets the operating system pick a free port; [`Server::local_addr`] reports which one.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be resolved or bound.
    pub fn bind<A: ToSocketAddrs, H: Handler>(self, addr: A, handler: H) -> io::Result<Server<H>> {
        let listener = std::net::TcpListener::bind(addr)?;
        let listener = async_net::TcpListener::try_from(listener)?;

        Ok(Server {
            listener,
            handler,
            limits: self.limits,
            threads: self.threads,
        })
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Accept connections from `listener` and spawn one task per connection on `executor`, until accepting fails.
async fn accept_connections<'a, H: Handler>(
    listener: &TcpListener,
    handler: &'a H,
    limits: Limits,
    executor: &Executor<'a>,
) -> io::Result<Infallible> {
    loop {
        let (stream, _peer) = listener.accept().await?;

        // send each response immediately, we write it in one piece anyway, so waiting for more data (Nagle) only adds delay
        // if this fails the connection still works, only a bit slower
        let _ = stream.set_nodelay(true);

        let task = serve_connection(stream.clone(), stream, handler, limits);
        executor.spawn(task).detach();
    }
}

/// Serve requests read from `input`, writing responses to `output`, until the connection closes or fails.
///
/// Errors end the connection.
/// Protocol errors get a final status-only response first, so the peer learns why; transport errors do not, because there is nothing left to write to.
/// Waiting longer than [`IDLE_TIMEOUT`] for the next request, or a read or write that exceeds [`CONNECTION_TIMEOUT`], ends the connection silently.
async fn serve_connection<R, W, H>(input: R, output: W, handler: &H, limits: Limits)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    H: Handler,
{
    let mut reader = RequestReader::new(input, limits);
    let mut writer = ResponseWriter::new(output);

    loop {
        let Some(waited) = with_timeout(reader.wait_for_request(), IDLE_TIMEOUT).await else {
            break;
        };

        // no request started yet, so there is nothing to answer
        if !matches!(waited, Ok(true)) {
            break;
        }

        let Some(read) = with_timeout(reader.read_request(), CONNECTION_TIMEOUT).await else {
            break;
        };

        let mut request = match read {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) => {
                if let Some(status) = error.client_status() {
                    let error_response = status_only(status);
                    let write = writer.write_response(error_response, false, true);

                    // ignore the error, we close the connection anyway
                    let _ = with_timeout(write, CONNECTION_TIMEOUT).await;
                }
                break;
            }
        };

        let keep_alive = wants_keep_alive(&request);

        // a HEAD gets the head of the GET response, so the handler answers it as a GET and its body is not sent
        let is_head = request.method() == Method::HEAD;
        if is_head {
            *request.method_mut() = Method::GET;
        }

        let response = handler.handle(request).await;

        let written = with_timeout(
            writer.write_response(response, keep_alive, !is_head),
            CONNECTION_TIMEOUT,
        )
        .await;

        if !matches!(written, Some(Ok(()))) || !keep_alive {
            break;
        }
    }

    // ignore the error, the client may already be gone
    let _ = with_timeout(writer.close(), CONNECTION_TIMEOUT).await;
}

/// Run `operation` to completion, or return `None` if it takes longer than `timeout`.
async fn with_timeout<T>(operation: impl Future<Output = T>, timeout: Duration) -> Option<T> {
    let operation_task = async { Some(operation.await) };
    let timeout_task = async {
        Timer::after(timeout).await;
        None
    };
    future::or(operation_task, timeout_task).await
}
