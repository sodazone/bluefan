//! Work queue services backend interfaces
use crate::{
	error::QResult,
	jobs::{Job, QueueName},
};

/// Interface required for work queue backend implementations.
pub trait WorkQueueBackend {
	fn start<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<()>;

	/// Triggered when a job is inserted into to a queue with reliability.
	fn put_job(&self, queue_name: &QueueName, job: &Job) -> QResult<()>;

	/// Triggered when a job is claimed.
	/// Moved from wait to run and sets expirations.
	fn claim_job(&self, queue_name: &QueueName, job: &Job) -> QResult<()>;

	/// Releases a claimed job.
	/// Moves the job from run to wait areas and cancels expirations.
	fn release_job(&self, queue_name: &QueueName, job_id: u64) -> QResult<Job>;

	/// Deletes a job from wait and running areas along with its associated time keys.
	fn delete_job(&self, queue_name: &QueueName, job_id: u64) -> QResult<()>;

	/// Triggered by a service background task.
	///
	/// This function enqueues the scheduled jobs at this time instant.
	/// Returns a relative milliseconds from UNIX epoch to wait for the next scheduled job.
	fn sched_tick<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<Option<u64>>;

	/// Triggered by a service background task.
	///
	/// Handles the time to run (TTR) expirations with retry logic.
	/// Returns a relative milliseconds from UNIX epoch to wait for the next TTR expiration.
	fn exp_tick<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<Option<u64>>;

	/// Returns an array of jobs from the waiting area.
	fn peek_job(&self, queue_name: &QueueName) -> QResult<Vec<Job>>;

	fn update_job(&self, queue_name: &QueueName, job: &Job) -> QResult<()>;

	fn complete_job(&self, queue_name: &QueueName, job: &Job) -> QResult<()>;

	/// Returns a running job.
	fn get_job_running(&self, queue_name: &QueueName, job_id: u64) -> QResult<Option<Job>>;

	/// Returns a completed job.
	fn get_job_completed(&self, queue_name: &QueueName, job_id: u64) -> QResult<Option<Job>>;

	// Returns a waiting job.
	fn get_job_waiting(&self, queue_name: &QueueName, job_id: u64) -> QResult<Option<Job>>;

	/// Resets the job time to run from now.
	fn touch_job(&self, queue_name: &QueueName, job_id: u64) -> QResult<()>;

	fn stop(&self);
}

/// Trait intented to be used by a coupled [WorkQueueBackend].
pub trait InnerWorkQueue {
	/// Inserts a job into the work queue.
	fn inner_put(&self, queue_name: QueueName, job: Job) -> QResult<u64>;
}
