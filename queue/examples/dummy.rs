use bluefan_queue::{jobs::Job, queue::WorkQueue, RocksBackend};
use tempfile::tempdir;

/// Dummy example.
fn main() {
	let backend = RocksBackend::new(&tmp_file());
	let work_queue = WorkQueue::new(backend);
	let job = Job::default();

	// Producer
	if let Ok(id) = work_queue.put("q1", &job) {
		// Consumer
		match work_queue.lease("q1") {
			Ok(Some(job)) => assert_eq!(job.id.unwrap(), id),
			_ => panic!(),
		}
	}
}

fn tmp_file() -> String {
	String::from(tempdir().unwrap().path().to_str().unwrap())
}
