use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("no upstream URL available")]
    NoUpstream,

    #[error("upstream HTTP error: {0}")]
    Upstream(#[from] reqwest::Error),

    #[error("invalid range header: {0}")]
    InvalidRange(String),

    #[error("upstream returned non-success status: {0}")]
    BadStatus(u16),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal: {0}")]
    Internal(String),
}

impl ProxyError {
    /// Whether this is the origin asking for less concurrency rather than
    /// reporting a broken transfer.
    ///
    /// Treated separately from ordinary failures because the answer is opposite:
    /// an ordinary failure should retry, promptly and elsewhere, while these
    /// want the pool to slow down and *not* spend its failure budget — a
    /// rate-limited origin is healthy, just busy.
    pub fn is_overload(&self) -> bool {
        matches!(self, Self::BadStatus(429 | 503))
    }
}

pub type Result<T> = std::result::Result<T, ProxyError>;
