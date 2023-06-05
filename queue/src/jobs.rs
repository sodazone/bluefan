//! Job data structures
use bytes::Bytes;
use getset::Setters;
use serde::{Deserialize, Serialize};

const DEFAULT_TTR_MILLIS: u32 = 180_000; // 30 minutes
const DEFAULT_RET_MILLIS: u32 = 360_000; // 1 hour

#[derive(Debug, Default, Clone, Serialize, Deserialize, Setters)]
#[getset(set = "pub")]
pub struct Job {
	pub id: Option<u64>,
	pub kind: Option<String>,
	pub args: Option<String>,
	pub attachment: Option<Bytes>,
	pub options: JobOptions,
	pub state: JobState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
	NONE = 0,
	WAIT = 1,
	RUNNING = 2,
	OK = 3,
	ERR = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
	pub ttr_ts: u64,
	pub delay_ts: u64,
	pub retention_ts: u64,
	pub retries_count: u32,
	pub status: JobStatus,
	pub outcome: Option<Bytes>,
	pub intermediate: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Setters)]
#[getset(set = "pub")]
pub struct JobOptions {
	pub durable: bool,
	/// time to run in milliseconds
	pub ttr: u32,
	/// delay in milliseconds
	pub delay: u32,
	/// maximum number of retries
	pub max_retries: u32,
	/// results retention in milliseconds
	pub retention: u32,
}

impl Default for JobOptions {
	fn default() -> Self {
		Self {
			durable: true,
			ttr: DEFAULT_TTR_MILLIS,
			max_retries: 0,
			delay: 0,
			retention: DEFAULT_RET_MILLIS,
		}
	}
}

impl Default for JobState {
	fn default() -> Self {
		Self {
			delay_ts: 0,
			ttr_ts: 0,
			retention_ts: 0,
			retries_count: 0,
			status: JobStatus::NONE,
			outcome: None,
			intermediate: None,
		}
	}
}

pub const QUEUE_NAME_LEN: usize = 32;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct QueueName {
	pub bytes: [u8; 32],
}

impl From<&str> for QueueName {
	fn from(value: &str) -> Self {
		let mut bytes = [0u8; 32];
		bytes[..value.len()].copy_from_slice(value.as_bytes());
		Self { bytes }
	}
}

impl From<&[u8]> for QueueName {
	fn from(value: &[u8]) -> Self {
		Self { bytes: value.try_into().unwrap() }
	}
}
