//! This crate provides access to [Bluefan Work Queue Services](queue).
//!
use std::time::UNIX_EPOCH;

pub mod backend;
pub mod error;
pub mod jobs;

pub mod queue;
pub use queue::{RocksWorkQueue, RocksWorkQueueService};

mod rocks;
pub use rocks::RocksBackend;

pub(crate) fn unix_millis() -> u64 {
	UNIX_EPOCH.elapsed().unwrap().as_millis() as u64
}

pub(crate) fn unix_secs() -> u64 {
	UNIX_EPOCH.elapsed().unwrap().as_secs()
}
