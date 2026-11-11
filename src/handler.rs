//! The request handler abstraction.

use std::future::Future;

use http::{Request, Response};

use crate::Body;

/// Turns a request into a response.
pub trait Handler: Send + Sync + 'static {
    /// Handle one request.
    ///
    /// Failure is not modelled here: a handler decides for itself what an error looks like and returns it as an ordinary response.
    /// A `HEAD` request arrives as a `GET`: the server sends the head of its response, with the `Content-Length` of the body, but not the body.
    fn handle(&self, request: Request<Body>) -> impl Future<Output = Response<Body>> + Send;
}

impl<F, Fut> Handler for F
where
    F: Fn(Request<Body>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send,
{
    fn handle(&self, request: Request<Body>) -> impl Future<Output = Response<Body>> + Send {
        self(request)
    }
}
