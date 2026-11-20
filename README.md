# tiny-http-server

A lightweight asynchronous HTTP/1.1 server.
It parses requests into `http::Request`, hands them to your handler, and writes the returned `http::Response` back.
A `HEAD` request reaches the handler as a `GET`, the server sends the head of its response without the body.
Routing, middleware, TLS and logging are left to the caller.

## Example

```rust,no_run
use std::num::NonZeroUsize;

use http::{Request, Response, StatusCode};
use tiny_http_server::{Body, ServerBuilder};

async fn route(request: Request<Body>) -> Response<Body> {
    match request.uri().path() {
        "/" => Response::new(Body::from("hello")),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::new())
            .unwrap(), // a status-only response is always valid
    }
}

ServerBuilder::new()
    .max_body_bytes(64 * 1024 * 1024)
    .threads(NonZeroUsize::new(4).unwrap())
    .bind("127.0.0.1:8080", route)
    .unwrap()
    .run()
    .unwrap();
```

`Server::bind(addr, route)` is the same with the defaults:

- `max_head_bytes`: the request line and headers, 16 KiB by default, a larger head gets `431 Request Header Fields Too Large`
- `max_body_bytes`: 1 MiB by default, a larger `Content-Length` gets `413 Content Too Large` before the body is read; the body is held in memory
- `threads`: the threads that serve the connections, the number of CPUs by default; the thread that calls `run` only accepts them, so a handler that blocks holds up only its own thread
