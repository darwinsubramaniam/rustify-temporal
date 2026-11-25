//! Like the Go frontend (frontend.maskInternalErrorDetails, on by default), Internal and Unknown
//! errors don't reach clients as they are: the message becomes "something went wrong, please
//! retry (<hash>)" without details, and the original is logged under that hash.

use std::convert::Infallible;

use tonic::body::Body;
use tonic::codegen::{BoxFuture, Context, Poll, Service, http};
use tonic::server::NamedService;

#[derive(Clone)]
pub struct MaskInternal<S>(pub S);

impl<S: NamedService> NamedService for MaskInternal<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for MaskInternal<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let method = req.uri().path().to_string();
        let fut = self.0.call(req);
        Box::pin(async move {
            let mut resp = fut.await?;
            mask(resp.headers_mut(), &method);
            Ok(resp)
        })
    }
}

fn mask(h: &mut http::HeaderMap, method: &str) {
    let code = h
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok());
    // 2 = Unknown, 13 = Internal.
    if !matches!(code, Some(2) | Some(13)) {
        return;
    }
    let original = h
        .get("grpc-message")
        .map(|v| percent_decode(v.as_bytes()))
        .unwrap_or_default();
    let hash = format!("{:08x}", fnv1a(original.as_bytes()));
    tracing::error!(method, hash, "internal error: {original}");
    let masked = format!("something went wrong, please retry ({hash})");
    if let Ok(v) = http::HeaderValue::from_str(&masked) {
        h.insert("grpc-message", v);
    }
    h.remove("grpc-status-details-bin");
}

fn percent_decode(b: &[u8]) -> String {
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or(""), 16)
        {
            out.push(v);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Correlates the client's message with the log line (the Go server uses farmhash here, so the
/// numbers differ between the servers).
fn fnv1a(b: &[u8]) -> u32 {
    b.iter()
        .fold(0x811c_9dc5u32, |h, &x| (h ^ x as u32).wrapping_mul(0x0100_0193))
}
