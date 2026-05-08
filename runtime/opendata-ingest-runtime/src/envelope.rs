//! Buffer-level wire format: the per-entry metadata header.
//!
//! Phase 4.2 ships only the type shapes the trait surface references
//! (`Decoder::accepts`). The parser (`decode_envelopes`,
//! `validate_consistent`) and the `ConfiguredEnvelope` matcher move
//! over from `clickhouse-ingestor::envelope` in Phase 4.3.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalType {
    Metrics,
    Logs,
    Traces,
}

impl SignalType {
    pub fn as_byte(self) -> u8 {
        match self {
            SignalType::Metrics => 1,
            SignalType::Logs => 2,
            SignalType::Traces => 3,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(SignalType::Metrics),
            2 => Some(SignalType::Logs),
            3 => Some(SignalType::Traces),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SignalType::Metrics => "metrics",
            SignalType::Logs => "logs",
            SignalType::Traces => "traces",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadEncoding {
    OtlpProtobuf,
}

impl PayloadEncoding {
    pub fn as_byte(self) -> u8 {
        match self {
            PayloadEncoding::OtlpProtobuf => 1,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(PayloadEncoding::OtlpProtobuf),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            PayloadEncoding::OtlpProtobuf => "otlp_protobuf",
        }
    }
}

/// Per-entry metadata envelope. Decoders match on this via
/// [`crate::decoder::Decoder::accepts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataEnvelope {
    pub version: u8,
    pub signal_type: SignalType,
    pub encoding: PayloadEncoding,
}
