use std::{
	sync::Arc,
	thread,
	time::{Duration, Instant},
};

use bluefan_queue::{
	error::QError,
	jobs::{Job, JobOptions, JobStatus},
	RocksWorkQueue, RocksWorkQueueService,
};
use crossbeam::scope;
use tempfile::tempdir;

const Q1: &str = "1";

#[test]
fn basic_put_lease() {
	let wk = wk();

	if let Ok(id) = wk.put(Q1, &Job::default()) {
		assert!(wk.get_running(Q1, id).unwrap().is_none());
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_some());
	} else {
		panic!()
	}
}

#[test]
fn basic_peek() {
	let wk = wk();

	assert!(wk.put(Q1, &Job::default()).is_ok());

	if let Ok(jobs) = wk.peek(Q1) {
		assert_eq!(jobs.len(), 1);
	} else {
		panic!()
	}

	if let Ok(jobs) = wk.peek("other") {
		assert_eq!(jobs.len(), 0);
	} else {
		panic!()
	}

	assert!(wk.lease(Q1).is_ok());

	if let Ok(jobs) = wk.peek(Q1) {
		assert_eq!(jobs.len(), 0);
	} else {
		panic!()
	}
}

#[test]
fn peek_delete() {
	let wk = wk();

	if let Ok(id) = wk.put(Q1, &Job::default()) {
		assert_eq!(wk.peek(Q1).unwrap().len(), 1);
		assert!(wk.delete(Q1, id).is_ok());
		assert_eq!(wk.peek(Q1).unwrap().len(), 0);
	} else {
		panic!()
	}
}

#[test]
fn lease_delete() {
	let wk = wk();

	if let Ok(id) = wk.put(Q1, &Job::default()) {
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_some());
		assert!(wk.delete(Q1, id).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_none());
	} else {
		panic!()
	}
}

#[test]
fn lease_release() {
	let wk = wk();

	if let Ok(id) = wk.put(Q1, &Job::default()) {
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_some());
		assert!(wk.release(Q1, id).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_none());
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.get_running(Q1, id).unwrap().is_some());
	} else {
		panic!()
	}
}

#[test]
fn lease_update() {
	let wk = wk();

	if let Ok(job_id) = wk.put(Q1, &Job::default()) {
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.touch_intermediate(Q1, job_id, Some("something".into())).is_ok());
		if let Ok(Some(job)) = wk.get_running(Q1, job_id) {
			assert_eq!(std::str::from_utf8(&job.state.intermediate.unwrap()), Ok("something"));
		} else {
			panic!()
		}
	} else {
		panic!()
	}
}

#[test]
fn lease_complete_ok() {
	let wk = wk();

	if let Ok(job_id) = wk.put(Q1, &Job::default()) {
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.complete_ok(Q1, job_id, Some("something".into())).is_ok());
		if let Ok(Some(job)) = wk.get_completed(Q1, job_id) {
			assert_eq!(job.state.status, JobStatus::OK);
			assert_eq!(std::str::from_utf8(&job.state.outcome.unwrap()), Ok("something"));
		} else {
			panic!()
		}
		assert!(wk.get_running(Q1, job_id).unwrap().is_none());
	} else {
		panic!()
	}
}

#[test]
fn lease_complete_fail() {
	let wk = wk();

	if let Ok(job_id) = wk.put(Q1, &Job::default()) {
		assert!(wk.lease(Q1).is_ok());
		assert!(wk.complete_fail(Q1, job_id, Some("bad".into())).is_ok());
		if let Ok(Some(job)) = wk.get_completed(Q1, job_id) {
			assert_eq!(job.state.status, JobStatus::ERR);
			assert_eq!(std::str::from_utf8(&job.state.outcome.unwrap()), Ok("bad"));
		} else {
			panic!()
		}
		assert!(wk.get_running(Q1, job_id).unwrap().is_none());
	} else {
		panic!()
	}
}

#[test]
fn concurrent_lease() {
	let wk = wk();

	scope(|scope| {
		for _ in 0..10 {
			scope.spawn(|_| {
				assert!(wk.put(Q1, &Job::default()).is_ok());
			});
		}
	})
	.unwrap();

	assert_eq!(wk.queue_len(Q1), 10);

	scope(|scope| {
		for _ in 0..10 {
			scope.spawn(|_| {
				assert!(wk.lease(Q1).is_ok());
			});
		}
	})
	.unwrap();

	assert_eq!(wk.queue_len(Q1), 0);
}

#[test]
fn concurrent_recv_and_delete() {
	let wk = wk();

	scope(|scope| {
		for _ in 0..10 {
			scope.spawn(|_| {
				assert!(wk.put(Q1, &Job::default()).is_ok());
			});
		}
	})
	.unwrap();

	assert_eq!(wk.queue_len(Q1), 10);

	scope(|scope| {
		for _ in 0..10 {
			scope.spawn(|_| {
				assert!(wk.recv_and_delete(Q1).is_ok());
			});
		}
	})
	.unwrap();

	assert_eq!(wk.queue_len(Q1), 0);
}

#[test]
fn basic_ttr() {
	let wk = wk();

	let mut job = Job::default();
	job.set_options(JobOptions { ttr: 500, max_retries: 1, ..JobOptions::default() });

	assert!(wk.put(Q1, &job).is_ok());

	match wk.lease(Q1) {
		Ok(Some(job)) => {
			assert_eq!(job.state.retries_count, 0)
		},
		_ => panic!(),
	}

	let start = Instant::now();

	thread::sleep(ms(500));

	while let Ok(j) = wk.lease(Q1) {
		match j {
			Some(j) => {
				assert_eq!(j.state.retries_count, 1);
				break;
			},
			None => {
				thread::sleep(ms(1));
			},
		}
	}

	let now = Instant::now();
	assert!(now - start >= ms(450));
	assert!(now - start <= ms(550));
}

#[test]
fn touch_ttr() {
	let wk = wk();

	let mut job = Job::default();
	job.set_options(JobOptions { ttr: 300, max_retries: 1, ..JobOptions::default() });

	assert!(wk.put(Q1, &job).is_ok());

	match wk.lease(Q1) {
		Ok(Some(job)) => {
			assert_eq!(job.state.retries_count, 0);
			let job_id = job.id.unwrap();
			let prev_ttr_ts = job.state.ttr_ts;
			assert_ne!(prev_ttr_ts, 0);

			thread::sleep(ms(100));

			assert!(wk.touch(Q1, job_id).is_ok());
			if let Ok(Some(job)) = wk.get_running(Q1, job_id) {
				assert!(job.state.ttr_ts - prev_ttr_ts > 90);
			} else {
				panic!()
			}
		},
		_ => panic!(),
	}
}

#[test]
fn basic_sched_delay() {
	let wk = wk();

	let mut job = Job::default();
	job.set_options(JobOptions { delay: 500, ..JobOptions::default() });

	assert!(wk.put(Q1, &job).is_ok());

	let start = Instant::now();

	thread::sleep(ms(100));

	match wk.lease(Q1) {
		Ok(None) => {},
		_ => panic!(),
	}

	thread::sleep(ms(400));

	while let Ok(j) = wk.lease(Q1) {
		match j {
			Some(j) => {
				assert_eq!(j.state.retries_count, 0);
				break;
			},
			None => {
				thread::sleep(ms(1));
			},
		}
	}

	let now = Instant::now();
	assert!(now - start >= ms(450));
	assert!(now - start <= ms(550));
}

#[test]
fn long_queue_name() {
	let wk = wk();

	if let Err(error) =
		wk.put("avrah kahdabra supercalifragilisticexpialidocious", &Job::default())
	{
		assert_eq!(
			error,
			QError::QueueError("the maximum queue name length is 32 bytes".into())
		);
	} else {
		panic!()
	}
}

#[test]
fn concurrent_queue_creation() {
	let wk = wk();

	scope(|scope| {
		for n in 0..100 {
			let wk = wk.clone();
			scope.spawn(move |_| {
				assert!(wk.put(format!("qn:{}", n).as_str(), &Job::default()).is_ok());
			});
		}
	})
	.unwrap();

	let wk = wk.clone();
	for n in 0..100 {
		assert_eq!(wk.queue_len(format!("qn:{}", n).as_str()), 1);
	}

	assert_eq!(wk.queue_len("qn:none"), 0);
}

fn ms(millis: u64) -> Duration {
	Duration::from_millis(millis)
}

fn wk() -> Arc<RocksWorkQueue> {
	let tmp_dir = tempdir().unwrap();
	let path = tmp_dir.path().to_str().unwrap();

	RocksWorkQueueService::new(path).queue()
}
