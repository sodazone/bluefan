//! gRPC server protocol implementation.
use bluefan_queue::jobs::{Job, JobOptions};
use bluefan_queue::RocksWorkQueueService;
use tonic::{Request, Response, Status};

use bluefan_rpc::queue_server::Queue;
use bluefan_rpc::{JobValue, LeaseRequest, PutRequest, PutResponse};

pub mod bluefan_rpc {
	tonic::include_proto!("bluefan.queue");
}

/// Converts a `Job` into a `JobValue`.
impl From<Job> for JobValue {
	fn from(job: Job) -> Self {
		Self {
			id: job.id.unwrap_or(0),
			status: job.state.status as i32,
			args: job.args.unwrap_or(Default::default()),
			kind: job.kind.unwrap_or(Default::default()),
		}
	}
}

/// Converts a `PutRequest` into a `Job`.
impl From<&PutRequest> for Job {
	fn from(val: &PutRequest) -> Self {
		let id = if val.job_id == 0 { None } else { Some(val.job_id) };
		let mut options = JobOptions { delay: val.delay, ..Default::default() };

		if val.ephemeral {
			options.durable = false;
			options.ttr = 0;
		} else {
			options.durable = true;
			if val.ttr > 0 {
				options.ttr = val.ttr
			}
		}

		Job {
			id,
			kind: if val.kind.is_empty() { None } else { Some(val.kind.to_string()) },
			args: if val.args.is_empty() { None } else { Some(val.args.to_string()) },
			attachment: None,
			options,
			state: Default::default(),
		}
	}
}

/// Implements the gRPC service `Queue` for handling queue operations.
pub struct QueueRpc {
	queue_service: RocksWorkQueueService,
}

impl QueueRpc {
	pub fn new(queue_service: RocksWorkQueueService) -> Self {
		Self { queue_service }
	}
}

#[tonic::async_trait]
impl Queue for QueueRpc {
	/// Handles the gRPC `put` method for adding a job to the queue.
	async fn put(
		&self,
		request: Request<PutRequest>,
	) -> Result<Response<PutResponse>, Status> {
		let body = &request.into_inner();
		let queue_name = &body.queue;
		let queue = self.queue_service.queue();
		let job: Job = body.into();

		match queue.put(queue_name, &job) {
			Ok(job_id) => Ok(Response::new(PutResponse { job_id })),
			Err(error) => Err(Status::internal(error.to_string())),
		}
	}

	/// Handles the gRPC `lease` method for leasing a job from the queue.
	async fn lease(
		&self,
		request: Request<LeaseRequest>,
	) -> Result<Response<JobValue>, Status> {
		let body = request.into_inner();
		let queue = self.queue_service.queue();

		match queue.lease(&body.queue) {
			Ok(maybe_job) => Ok(Response::new(maybe_job.unwrap_or_default().into())),
			Err(error) => Err(Status::internal(error.to_string())),
		}
	}
}
