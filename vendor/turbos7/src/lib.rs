//! # turbos7
//!
//! Асинхронная библиотека для работы с Siemens S7 PLC.
pub mod types;

pub use types::ReadItem;
pub use types::ReadResult;
pub use types::WriteItem;

pub mod client;
pub mod connection;
pub mod error;
pub mod protocol;


pub use client::S7Client;
pub use error::Error;

/// Тип Result, используемый в библиотеке.
pub type Result<T> = std::result::Result<T, Error>;

pub use tracing;
pub use crate::protocol::s7;