//! Common result and error

use std::fmt::{Display, Formatter, Result};
pub type QResult<T> = std::result::Result<T, QError>;

#[derive(Debug)]
pub enum QError {
	StoreError(String),
	QueueError(String),
}

impl From<std::string::String> for QError {
	fn from(value: std::string::String) -> Self {
		Self::QueueError(value)
	}
}

impl Display for QError {
	fn fmt(&self, f: &mut Formatter) -> Result {
		match self {
			QError::StoreError(msg) => {
				write!(f, "Store error: {}", msg)
			},
			QError::QueueError(msg) => {
				write!(f, "Queue error: {}", msg)
			},
		}
	}
}
