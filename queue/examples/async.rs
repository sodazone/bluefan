use std::{
	sync::{
		atomic::{AtomicU32, Ordering},
		Arc,
	},
	time::Instant,
};

use async_std::task;
use bluefan_queue::{
	jobs::{Job, JobOptions},
	RocksWorkQueueService,
};
use log::{debug, info};
use tempfile::tempdir;

const Q1: &str = "1";

fn sample_job(p: u32, n: u32) -> Job {
	Job::default()
		.set_args(Some(format!(r#"{{"arg1": "hello", "num": {}}}"#, n)))
		.set_kind(Some(format!("p:{}", p)))
		.set_options(JobOptions { durable: true, retention: 0, ..Default::default() })
		.clone()
}

#[async_std::main]
async fn main() {
	env_logger::init();

	info!("Std Async Example");

	let producers_num = 10;
	let workers_num = 50;
	let jobs_num = 100_000;

	let total_jobs = producers_num * jobs_num;
	let burndown = Arc::new(AtomicU32::new(total_jobs));
	let wk = RocksWorkQueueService::new(&tmp_file());

	info!("producers={} consumers={} jobs={}", producers_num, workers_num, total_jobs);

	for p in 1..=producers_num {
		task::spawn({
			let wk = wk.queue();
			async move {
				for n in 1..=jobs_num {
					assert!(wk.put(Q1, &sample_job(p, n)).is_ok());
				}
			}
		});
	}

	let tasks: Vec<_> = (1..=workers_num)
		.enumerate()
		.map(|(_, i)| {
			task::spawn({
				let wk = wk.queue();
				let burndown = burndown.clone();
				async move {
					// Acquires the lease
					while let Ok(maybe_job) = wk.lease(Q1) {
						match maybe_job {
							Some(job) => {
								// Completes the job
								assert!(wk.complete_ok(Q1, job.id.unwrap(), None).is_ok());
								// Updates the work burndown counter
								let remaining = burndown.fetch_sub(1, Ordering::Relaxed);
								debug!("Worker {} - Completed {:?} [{}]", i, job, remaining);
							},
							None => task::yield_now().await,
						}
						if burndown.load(Ordering::Relaxed) == 0 {
							break;
						}
					}
				}
			})
		})
		.collect();

	let now = Instant::now();

	futures::future::join_all(tasks).await;

	let elapsed = now.elapsed();
	info!("{}s {}ms {}ns", elapsed.as_secs(), elapsed.as_millis(), elapsed.as_nanos());
}

fn tmp_file() -> String {
	String::from(tempdir().unwrap().path().to_str().unwrap())
}
