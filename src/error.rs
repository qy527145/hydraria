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

pub type Result<T> = std::result::Result<T, ProxyError>;
