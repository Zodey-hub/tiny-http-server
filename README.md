# tiny-http-server

A lightweight asynchronous HTTP/1.1 server.
It parses requests into `http::Request`, hands them to your handler, and writes the returned `http::Response` back.
A `HEAD` request reaches the handler as a `GET`, the server sends the head of its response without the body.
Routing, middleware, TLS and logging are left to the caller.

## Example

```rust,no_run
use http::{Request, Response, StatusCode};
use tiny_http_server::{Body, Server};

async fn route(request: Request<Body>) -> Response<Body> {
    match request.uri().path() {
        "/" => Response::new(Body::from("hello")),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::new())
            .unwrap(), // a status-only response is always valid
    }
}

Server::bind("127.0.0.1:8080", route).unwrap().run().unwrap();
```
