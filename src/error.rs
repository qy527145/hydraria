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

    /// The origin is asking for less load: 429/503, optionally with a
    /// `Retry-After` the scheduler should honour.
    #[error("upstream is throttling (status {status})")]
    Throttled {
        status: u16,
        retry_after: Option<std::time::Duration>,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A URL served a different `ETag` than it did earlier in this download, so
    /// the entity changed under us and its bytes can no longer be stitched
    /// together with what we already have.
    #[error("upstream content changed mid-download: {url}")]
    ContentChanged { url: String },

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
        matches!(self, Self::Throttled { .. } | Self::BadStatus(429 | 503))
    }

    /// How long the origin asked us to wait, if it said so at all.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Throttled { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Whether the entity changed under us.
    ///
    /// Kept distinct from a generic failure because it must **not** reach the
    /// automatic claim-size learner: that learner reads "a claim failed having
    /// delivered nothing" as "this range was too big", and would shrink claims
    /// (and now persist that wall for minutes) in response to something that
    /// says nothing whatsoever about size.
    pub fn is_content_changed(&self) -> bool {
        matches!(self, Self::ContentChanged { .. })
    }
}

pub type Result<T> = std::result::Result<T, ProxyError>;
