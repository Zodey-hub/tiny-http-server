//! Connection acceptance and the per-connection request loop.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use async_executor::Executor;
use async_io::Timer;
use async_net::{AsyncToSocketAddrs, TcpListener};
use futures_lite::{AsyncRead, AsyncWrite, future};
use http::Method;

use crate::request::{RequestReader, wants_keep_alive};
use crate::response::{ResponseWriter, status_only};
use crate::{CONNECTION_TIMEOUT, Error, Handler, IDLE_TIMEOUT};

/// An HTTP/1.1 server that dispatches every request to a single [`Handler`].
pub struct Server<H> {
    listener: TcpListener,
    handler: H,
}

impl<H: Handler> Server<H> {
    /// Bind `addr` and create a server that routes every request to `handler`.
    ///
    /// `addr` is any address `async-net` can resolve, such as `"127.0.0.1:8080"`.
    /// Binding port `0` lets the operating system pick a free port; [`Server::local_addr`] reports which one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the address cannot be bound.
    pub fn bind<A: AsyncToSocketAddrs>(addr: A, handler: H) -> Result<Self, Error> {
        let listener = async_io::block_on(TcpListener::bind(addr))?;
        Ok(Self { listener, handler })
    }

    /// The address the listener is bound to, including the port the operating system picked when port `0` was requested.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the operating system cannot report the address.
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        Ok(self.listener.local_addr()?)
    }

    /// Serve connections on the current thread until an accept error occurs.
    ///
    /// Runs every connection on a single-threaded executor owned by this call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the listener stops accepting.
    /// The success arm is unreachable: on a healthy listener this call never returns.
    pub fn run(&self) -> Result<(), Error> {
        let executor = Executor::new();
        let accept_loop = accept_connections(&self.listener, &self.handler, &executor);
        async_io::block_on(executor.run(accept_loop))
    }
}

/// Accept connections from `listener` and spawn one task per connection on `executor`, until accepting fails.
async fn accept_connections<'a, H: Handler>(
    listener: &TcpListener,
    handler: &'a H,
    executor: &Executor<'a>,
) -> Result<(), Error> {
    loop {
        let (stream, _peer) = listener.accept().await?;

        // send each response immediately, we write it in one piece anyway, so waiting for more data (Nagle) only adds delay
        // if this fails the connection still works, only a bit slower
        let _ = stream.set_nodelay(true);

        let task = serve_connection(stream.clone(), stream, handler);
        executor.spawn(task).detach();
    }
}

/// Serve requests read from `input`, writing responses to `output`, until the connection closes or fails.
///
/// Errors end the connection.
/// Protocol errors get a final status-only response first, so the peer learns why; transport errors do not, because there is nothing left to write to.
/// Waiting longer than [`IDLE_TIMEOUT`] for the next request, or a read or write that exceeds [`CONNECTION_TIMEOUT`], ends the connection silently.
async fn serve_connection<R, W, H>(input: R, output: W, handler: &H)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    H: Handler,
{
    let mut reader = RequestReader::new(input);
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
