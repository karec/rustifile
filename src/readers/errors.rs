use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Reader error : {0}")]
    InitializationError(&'static str),
}
