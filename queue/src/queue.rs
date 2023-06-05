//! # Work Queue Services
//!
//! Bluefan Work Queue Services offer fast and reliable work queues for distributed job processing.
//!
//! ## Features
//!
//! - First in, first out (FIFO) job delivery for concurrent competing consumers
//! - Persistence of jobs for system restarts or failures
//! - Automatic job retry and time limit for execution
//! - Support for at-most-once and at-least-once work processing semantics
//!
//! See the [bluefan-rpc] crate for a networked server implementation.
//!
//! ## Example
//!
//! ```no_run
//! use bluefan_queue::{
//!    jobs::Job,
//!    RocksWorkQueueService
//! };
//!
//! let work_queue = RocksWorkQueueService::new("./.db").queue();
//!
//! // Producer puts a job
//! work_queue.put("q1", &Job::default());
//!
//! // Consumer leases the job
//! if let Ok(Some(job)) = work_queue.lease("q1") {
//!     println!("{:?}", job);
//! }
//! ```
//!
//! [bluefan-rpc]: crate::bluefan_rpc
use std::{
	sync::{
		atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
		Arc,
	},
	time::Duration,
};

use bytes::Bytes;
use crossbeam::channel::{bounded, select, Receiver, Sender};

use backend::InnerWorkQueue;
use crossbeam::queue::SegQueue;
use dashmap::{mapref::entry::Entry, DashMap};
use jobs::JobState;
use log::{debug, info, warn};

use crate::{
	backend::{self, WorkQueueBackend},
	error::QResult,
	jobs::{self, Job, JobStatus, QueueName},
	rocks::RocksBackend,
	unix_millis, unix_secs,
};

static CURRENT_EPOCH: AtomicU64 = AtomicU64::new(0);
static HEAD: AtomicU32 = AtomicU32::new(0);

/// Bluefan Work Queue.
///
/// The WorkQueue struct exposes a public interface for job producers and consumers.
/// It provides FIFO named queues for fast concurrent job distribution and processing.
///
/// The WorkQueue relies on a [WorkQueueBackend] implementation for persistence and scheduling.
///
/// Thread safety is guaranteed for read operations and for multiple concurrent consumers,
/// such as using the [Self::lease()] or [Self::recv_and_delete()] methods.
///
/// It is important to note that a leased [Job] is expected to be processed by a single consumer.
#[derive(Debug)]
pub struct WorkQueue<S>
where
	S: WorkQueueBackend,
{
	backend: S,
	queues: DashMap<QueueName, SegQueue<Job>>,
	exp_next: AtomicU64,
	exp_chan: (Sender<bool>, Receiver<bool>),
	sched_next: AtomicU64,
	sched_chan: (Sender<bool>, Receiver<bool>),
	shutdown: AtomicBool,
}

/// Provides shared access to the underlying [WorkQueue].
#[derive(Debug, Clone)]
pub struct WorkQueueService<S>
where
	S: WorkQueueBackend,
{
	q: Arc<WorkQueue<S>>,
}

impl<S: WorkQueueBackend> WorkQueue<S> {
	pub fn new(backend: S) -> Self {
		Self {
			backend,
			queues: DashMap::new(),
			exp_chan: bounded(1),
			exp_next: AtomicU64::new(u64::MAX),
			sched_chan: bounded(1),
			sched_next: AtomicU64::new(u64::MAX),
			shutdown: AtomicBool::new(false),
		}
	}

	/// Returns the number of jobs waiting in a in-memory queue.
	pub fn queue_len(&self, queue_name: &str) -> u64 {
		if let Some(queue) = self.queues.get(&queue_name.into()) {
			queue.len() as u64
		} else {
			0
		}
	}

	/// Inserts a [Job] into the queue.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue where the job will be inserted.
	/// - `job`: A reference to a [Job] object representing the job to be inserted.
	///
	/// Returns:
	/// - `QResult<u64>`: A result that contains the ID of the inserted job.
	///   If a job ID is not provided, a new ID will be generated for the job.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn put(&self, queue_name: &str, job: &Job) -> QResult<u64> {
		let id = job.id.unwrap_or(self.gen_id());
		let options = job.options.clone();
		let delay_ts =
			if options.delay > 0 { unix_millis() + options.delay as u64 } else { 0 };

		self.inner_put(
			queue_name.into(),
			Job {
				id: Some(id),
				attachment: job.attachment.clone(),
				kind: job.kind.clone(),
				args: job.args.clone(),
				options,
				state: JobState { delay_ts, status: JobStatus::WAIT, ..JobState::default() },
			},
		)
	}

	/// Returns a running job.
	///
	/// This method allows you to retrieve information about a running job from the queue.
	/// A running job refers to a job that has been leased but has not yet been completed or failed.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the running job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the running job to retrieve.
	///
	/// Returns:
	/// - `QResult<Option<Job>>`: A result that contains an `Option` wrapping the running job, or `None` if the job is not found.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn get_running(&self, queue_name: &str, job_id: u64) -> QResult<Option<Job>> {
		self.backend.get_job_running(&queue_name.into(), job_id)
	}

	/// Retrieves a completed job from a queue.
	///
	/// This method allows you to retrieve information about a completed job from the queue.
	/// A completed job refers to a job that has finished processing, either successfully or not, and is no longer running.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the completed job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the completed job to retrieve.
	///
	/// Returns:
	/// - `QResult<Option<Job>>`: A result that contains an `Option` wrapping the completed job, or `None` if the job is not found.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn get_completed(&self, queue_name: &str, job_id: u64) -> QResult<Option<Job>> {
		self.backend.get_job_completed(&queue_name.into(), job_id)
	}

	/// Marks a job as successfully completed.
	///
	/// This method marks a job as successfully completed in the specified queue.
	/// It is used to indicate that a job has finished processing without any errors.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the job to mark as completed.
	/// - `outcome`: An optional reference to a byte slice representing additional data associated with the completed job.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of marking the job as completed.
	///   It returns `Ok(())` if the job is successfully marked as completed, or an error if the operation fails.
	pub fn complete_ok(
		&self,
		queue_name: &str,
		job_id: u64,
		outcome: Option<Bytes>,
	) -> QResult<()> {
		if let Ok(Some(mut job)) = self.get_running(queue_name, job_id) {
			job.state.status = JobStatus::OK;
			job.state.outcome = outcome;

			self.backend.complete_job(&queue_name.into(), &job)
		} else {
			Ok(())
		}
	}

	/// Marks a job as failed.
	///
	/// This method marks a job as failed in the specified queue.
	/// It is used to indicate that a job has encountered an error or failed during processing.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the job to mark as failed.
	/// - `outcome`: An optional reference to a byte slice representing additional data associated with the failed job.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of marking the job as failed.
	///   It returns `Ok(())` if the job is successfully marked as failed, or an error if the operation fails.
	pub fn complete_fail(
		&self,
		queue_name: &str,
		job_id: u64,
		outcome: Option<Bytes>,
	) -> QResult<()> {
		if let Ok(Some(mut job)) = self.get_running(queue_name, job_id) {
			job.state.status = JobStatus::ERR;
			job.state.outcome = outcome;

			self.backend.complete_job(&queue_name.into(), &job)
		} else {
			Ok(())
		}
	}

	/// Updates the intermediate data of a running job and extends the lease time.
	///
	/// This method allows you to update the intermediate data of a running job in the specified queue.
	/// Additionally, it extends the time to run of the lease, similar to the behavior of [Self::touch()] method.
	/// It is used to modify or add additional information to a job while it is still in progress.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the running job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the running job to update.
	/// - `intermediate`: An optional reference to a byte slice representing the intermediate data to update the job with.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of updating the intermediate data of the running job.
	///   It returns `Ok(())` if the job is successfully updated, or an error if the operation fails.
	pub fn touch_intermediate(
		&self,
		queue_name: &str,
		job_id: u64,
		intermediate: Option<Bytes>,
	) -> QResult<()> {
		if let Ok(Some(mut job)) = self.get_running(queue_name, job_id) {
			job.state.intermediate = intermediate;
			self.backend.update_job(&queue_name.into(), &job)
		} else {
			Ok(())
		}
	}

	/// Deletes a job by ID.
	///
	/// This method allows you to delete a specific job from the queue based on its ID.
	/// Please note that this method only deletes jobs from storage and does not remove it from memory.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue from which the job needs to be deleted.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the job to be deleted.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of the delete operation.
	///   It returns `Ok(())` if the job is successfully deleted, or an error if the operation fails.
	pub fn delete(&self, queue_name: &str, job_id: u64) -> QResult<()> {
		self.backend.delete_job(&queue_name.into(), job_id)
	}

	/// Receive and delete.
	///
	/// This method allows for the simplest mode of consumption, where a job is received and immediately deleted from the queue.
	/// Provides at-most-once semantics for consumers.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue from which to receive the job.
	///
	/// Returns:
	/// - `QResult<Option<Job>>`: A result that contains an `Option` wrapping the received job, or `None` if the queue is empty.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn recv_and_delete(&self, queue_name: &str) -> QResult<Option<Job>> {
		if let Some(q) = self.queues.get(&queue_name.into()) {
			match q.pop() {
				Some(job) => {
					let job_id = job.id.unwrap();
					if job.options.ttr > 0 {
						warn!(
							"Receive and delete of a job with leasing time. Job ID: {}",
							job_id
						)
					}
					if job.options.durable {
						self.delete(queue_name, job_id)?
					}
					Ok(Some(job))
				},
				None => Ok(None),
			}
		} else {
			Ok(None)
		}
	}

	/// Delegates to [Self::lease_with_time()] without overriding the job time-to-run.
	///
	/// This method is a convenience wrapper that delegates to the [Self::lease_with_time()] method without overriding the time-to-run (TTR) value of the job.
	/// It allows you to lease a waiting job from the queue, following the same two-stage, reliable at-least-once processing semantics as [Self::lease_with_time()].
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue from which to lease the job.
	///
	/// Returns:
	/// - `QResult<Option<Job>>`: A result that contains an `Option` wrapping the leased job, or `None` if no job is available.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn lease(&self, queue_name: &str) -> QResult<Option<Job>> {
		self.lease_with_time(queue_name, None)
	}

	/// Leases a waiting job with a specified time to run (TTR).
	///
	/// This method allows you to lease a waiting job from the specified queue, providing reliable at-least-once processing semantics.
	/// A waiting job refers to a job that is in the queue and ready to be processed.
	///
	/// A leased job is expected to be processed by a single consumer within the specified time to run (TTR) window.
	/// The TTR determines the maximum allowed execution time for the job. The TTR can be extended by calling [Self::touch()]
	/// or [Self::touch_intermediate()].
	///
	/// When the job processing is successfully finished, you should mark the job as complete by calling [Self::complete_ok()].
	/// The completed job will be retained according to the retention time defined in the job options.
	///
	/// If the time to run expires before the job is completed, the job will be re-enqueued until the maximum retries
	/// configured in the job options are reached. If the maximum retries are exceeded, the job will fail.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue from which to lease the job.
	/// - `ttr`: An optional parameter of type `Option<u32>` representing the Time To Run (TTR) value for the leased job.
	///           If specified, the TTR determines the maximum allowed execution time for the job.
	///           Otherwise, the TTR defined in the job creation will apply.
	///
	/// Returns:
	/// - `QResult<Option<Job>>`: A result that contains an `Option` wrapping the leased job, or `None` if no job is available.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn lease_with_time(&self, queue_name: &str, ttr: Option<u32>) -> QResult<Option<Job>> {
		let qn = &queue_name.into();

		if let Some(q) = self.queues.get(qn) {
			q.pop()
				.map(|mut job| {
					job.options.ttr = ttr.unwrap_or(job.options.ttr);

					if job.options.ttr > 0 {
						job.state.status = JobStatus::RUNNING;
						let ttr = unix_millis() + job.options.ttr as u64;
						job.state.ttr_ts = ttr;
						self.backend.claim_job(qn, &job)?;
						self.sched_ttr(ttr);

						Ok(Some(job))
					} else {
						job.state.status = JobStatus::OK;
						if job.options.durable {
							self.backend.delete_job(qn, job.id.unwrap())?;
						}
						Ok(Some(job))
					}
				})
				.unwrap_or(Ok(None))
		} else {
			Ok(None)
		}
	}

	/// Releases a job back to the waiting queue.
	///
	/// This method allows you to release a job back to the queue after it has been leased, making it available for other consumers to process.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue to which the job will be released.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the job to be released.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of the release operation.
	///   It returns `Ok(())` if the job is successfully released, or an error if the operation fails.
	pub fn release(&self, queue_name: &str, job_id: u64) -> QResult<u64> {
		let qn = queue_name.into();

		match self.backend.release_job(&qn, job_id) {
			Ok(job) => self.inner_put(qn, job),
			Err(error) => Err(error),
		}
	}

	/// Returns waiting jobs without removing them.
	///
	/// This method allows you to retrieve a list of waiting jobs from the queue without actually consuming them.
	/// It specifically applies to durable jobs that are available in the waiting area of the queue.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue from which to retrieve the waiting jobs.
	///
	/// Returns:
	/// - `QResult<Vec<Job>>`: A result that contains a vector of `Job` objects representing the waiting jobs.
	///   The result can also indicate any potential errors that may occur during the operation.
	pub fn peek(&self, queue_name: &str) -> QResult<Vec<Job>> {
		self.backend.peek_job(&queue_name.into())
	}

	/// Extends the time to run of a leased job.
	///
	/// This method allows you to extend the time to run of a job that is currently leased, i.e. in the running state.
	/// By invoking this method, you can prevent the job from being automatically released or re-queued when the original time to run expires.
	///
	/// Parameters:
	/// - `queue_name`: A string representing the name of the queue containing the running job.
	/// - `job_id`: An unsigned 64-bit integer representing the ID of the running job to extend the time to run.
	///
	/// Returns:
	/// - `QResult<()>`: A result indicating the success or failure of extending the time to run for the job.
	///   It returns `Ok(())` if the time to run is successfully extended, or an error if the operation fails.
	pub fn touch(&self, queue_name: &str, job_id: u64) -> QResult<()> {
		self.backend.touch_job(&queue_name.into(), job_id)?;
		self.notify_ttr();

		Ok(())
	}

	fn start(&self) -> QResult<()> {
		info!("Queue Services: start");

		self.update_epoch();

		self.backend.start(self)
	}

	fn is_running(&self) -> bool {
		!self.is_shutdown()
	}

	fn is_shutdown(&self) -> bool {
		self.shutdown.load(Ordering::Relaxed)
	}

	fn stop(&self) {
		info!("Queue Services: stop");

		self.backend.stop();

		info!("Queue Services: end");
	}

	fn handle_exp(&self) -> QResult<Option<u64>> {
		if self.is_shutdown() {
			return Ok(None);
		}

		self.backend.exp_tick(self)
	}

	fn handle_scheduled(&self) -> QResult<Option<u64>> {
		if self.is_shutdown() {
			return Ok(None);
		}

		self.backend.sched_tick(self)
	}

	fn sched_ttr(&self, ttr: u64) {
		if ttr < self.exp_next.load(Ordering::Relaxed) {
			self.exp_next.store(ttr, Ordering::Relaxed);
			self.notify_ttr();
		};
	}

	fn gen_id(&self) -> u64 {
		if HEAD.load(Ordering::SeqCst) == u32::MAX {
			self.update_epoch()
		}

		CURRENT_EPOCH.load(Ordering::SeqCst) + HEAD.fetch_add(1, Ordering::SeqCst) as u64
	}

	fn update_epoch(&self) {
		let epoch = unix_secs();
		CURRENT_EPOCH.store(epoch, Ordering::SeqCst);

		info!("> Epoch: {}", epoch);
	}

	fn notify_ttr(&self) {
		self.exp_chan.0.send(true).unwrap()
	}

	fn notify_scheduled(&self) {
		self.sched_chan.0.send(true).unwrap()
	}
}

impl<S: WorkQueueBackend> InnerWorkQueue for WorkQueue<S> {
	/// Puts a job into the in-memory queue and creates the queue if it does not exist.
	///
	/// This method handles the following operations:
	/// - Delayed job scheduling
	/// - Durable persistence
	///
	/// Parameters:
	/// - `queue_hash`: An unsigned 32-bit integer representing the hash of the queue.
	/// - `job`: The job to be put into the queue.
	///
	/// Returns:
	/// - `QResult<u64>`: A result indicating the success or failure of putting the job into the queue.
	///   It returns `Ok(job_id)` if the job is successfully put into the queue, where `job_id` represents the ID of the job,
	///   or an error if the operation fails.
	fn inner_put(&self, queue_name: QueueName, job: Job) -> QResult<u64> {
		let job_id = job.id.expect("job with id");

		if job.options.durable {
			self.backend.put_job(&queue_name, &job)?;
		}

		if job.options.delay > 0
			&& job.state.delay_ts < self.sched_next.load(Ordering::Relaxed)
		{
			self.sched_next.store(job.state.delay_ts, Ordering::Relaxed);
			self.notify_scheduled();
		} else {
			match self.queues.entry(queue_name) {
				Entry::Occupied(occuppied) => {
					let q = occuppied.get();
					q.push(job);
				},
				Entry::Vacant(vacant) => {
					let q = SegQueue::new();
					q.push(job);
					vacant.insert(q);
				},
			}
		}

		Ok(job_id)
	}
}

impl<S: WorkQueueBackend + Send + Sync + 'static> WorkQueueService<S> {
	pub(crate) fn new(db: S) -> Self {
		let q = Arc::new(WorkQueue::new(db));

		// expirations task
		std::thread::spawn({
			let q = Arc::clone(&q);
			move || Self::exp_task(q)
		});

		// scheduled jobs scheduling task
		std::thread::spawn({
			let q = Arc::clone(&q);
			move || Self::sched_task(q)
		});

		q.start().expect("queue start");

		Self { q }
	}

	fn exp_task(q: Arc<WorkQueue<S>>) {
		while q.is_running() {
			debug!("tick: exp");

			if let Ok(Some(next_tick)) = q.handle_exp() {
				select!(
					recv(q.exp_chan.1) -> _ => {
						debug!("notify: exp");
					},
					default(Duration::from_millis(next_tick)) => {}
				)
			} else {
				q.exp_chan.1.recv().unwrap();
			}
		}
	}

	fn sched_task(q: Arc<WorkQueue<S>>) {
		while q.is_running() {
			debug!("tick: sched");

			if let Ok(Some(next_tick)) = q.handle_scheduled() {
				select!(
					recv(q.sched_chan.1) -> _ => {
						debug!("notify: sched");
					},
					default(Duration::from_millis(next_tick)) => {}
				)
			} else {
				q.sched_chan.1.recv().unwrap();
			}
		}
	}

	pub fn queue(&self) -> Arc<WorkQueue<S>> {
		debug!("clone: inner queue");

		Arc::clone(&self.q)
	}
}

impl<S: WorkQueueBackend> Drop for WorkQueueService<S> {
	fn drop(&mut self) {
		debug!("drop: queue service");

		self.q.stop();
	}
}

/// RocksDB backed [WorkQueue].
pub type RocksWorkQueue = WorkQueue<RocksBackend>;
pub type InnerRocksWorkQueueService = WorkQueueService<RocksBackend>;

/// RocksDB backed [WorkQueueService].
pub struct RocksWorkQueueService {
	qs: InnerRocksWorkQueueService,
}

impl RocksWorkQueueService {
	pub fn new(path: &str) -> Self {
		Self { qs: WorkQueueService::new(RocksBackend::new(path)) }
	}

	pub fn queue(&self) -> Arc<RocksWorkQueue> {
		self.qs.queue()
	}
}
