use bluefan_queue::{jobs::Job, RocksWorkQueueService};
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

const QN: &str = "abc";

fn bench_queue(c: &mut Criterion) {
	let tmp_dir = tempdir().unwrap();
	let path = tmp_dir.path().to_str().unwrap();
	let wk = RocksWorkQueueService::new(path).queue();

	c.bench_function("queue put (ephemeral)", |b| {
		let mut job = Job::default();
		job.options.set_durable(false);

		b.iter(|| wk.put(QN, &job));
	});

	c.bench_function("queue put (persistent)", |b| {
		let job = Job::default();

		b.iter(|| wk.put(QN, &job));
	});

	c.bench_function("queue peek", |b| {
		b.iter(|| assert!(wk.peek(QN).expect("peek").len() > 0));
	});

	c.bench_function("queue reserve", |b| {
		b.iter(|| match wk.lease(QN) {
			Ok(_) => {},
			Err(error) => panic!("{:?}", error),
		});
	});

	// TODO: we will need to populate with fixed ids
	// c.bench_function("get running", || match wk.get(QN, ))
}

criterion_group!(benches, bench_queue);
criterion_main!(benches);
