use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Timeout")]
    Timeout,

    #[error("Invalid data")]
    InvalidData,

    /// Ошибка, возвращённая самим ПЛК
    #[error("PLC returned error: 0x{code:04X} ({message})")]
    PlcError {
        code: u16,
        message: String,
    },
}