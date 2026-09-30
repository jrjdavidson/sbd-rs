use std::path::PathBuf;

use thiserror::Error;

use crate::mo::Header;

/// Crate-specific error enum.
#[derive(Debug, Error)]
pub enum Error {
    /// The identifier is invalid.
    #[error("invalid information element identifier: {0}")]
    InvalidInformationElementIdentifier(u8),

    /// The message has an invalid protocol revision number.
    #[error("invalid protocol revision number: {0}")]
    InvalidProtocolRevisionNumber(u8),

    /// Invalid time of session.
    #[error("invalid time of session")]
    InvalidTimeOfSession,

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The overall message length is too big.
    #[error("the overall message length is too big: {0}")]
    OverallMessageLength(usize),

    /// The timestamp is negative.
    #[error("negative timestamp: {0}")]
    NegativeTimestamp(i64),

    /// There is no header in the message.
    #[error("no header")]
    NoHeader,

    /// The path is not a directory.
    #[error("not a directory: {}", .0.display())]
    NotADirectory(PathBuf),

    /// There is no payload in the message.
    ///
    /// Carries the message header so the sending device can be identified. Note that Iridium
    /// sends MO messages without a payload for sessions with an empty MO buffer (e.g. a mailbox
    /// check).
    #[error(
        "no payload in message from IMEI {} (MOMSN {}, MTMSN {}, session status {:?}, time of session {})",
        String::from_utf8_lossy(&.0.imei),
        .0.momsn,
        .0.mtmsn,
        .0.session_status,
        .0.time_of_session
    )]
    NoPayload(Header),

    /// The payload is too long.
    #[error("the payload is too long at {0} bytes")]
    PayloadTooLong(usize),

    /// There are two headers in the message.
    #[error("two headers")]
    TwoHeaders(Header, Header),

    /// There are two payloads in the message.
    #[error("two payloads")]
    TwoPayloads(Vec<u8>, Vec<u8>),

    /// Unknown session status code.
    #[error("unknown session status: {0}")]
    UnknownSessionStatus(u8),

    /// Walkdir error.
    #[error("walkdir error: {0}")]
    Walkdir(#[from] walkdir::Error),
}
