use thiserror::Error;
use aws_sdk_s3::primitives::ByteStreamError;
use parquet::errors::ParquetError;
use arrow::error::ArrowError;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::client::result::CreateUnhandledError;
use aws_smithy_runtime_api::http::Response;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] rquest::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("S3 error: {0}")]
    S3(#[from] aws_sdk_s3::Error),

    #[error("AWS SDK error: {0}")]
    AwsSdk(String),
    
    #[error("Rate limit exceeded")]
    RateLimit,

    #[error("Forbidden - Access denied")]
    Forbidden,

    #[error("Maximum retries exceeded")]
    MaxRetriesExceeded,

    #[error("Lock error: {0}")]
    Lock(#[from] tokio::sync::TryLockError),
    
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),
    
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("ByteStream error: {0}")]
    ByteStream(#[from] ByteStreamError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
}

// Implement From for various SdkError types
impl<E: std::fmt::Debug + CreateUnhandledError> From<SdkError<E, Response>> for Error {
    fn from(err: SdkError<E, Response>) -> Self {
        Error::AwsSdk(format!("{:?}", err))
    }
}