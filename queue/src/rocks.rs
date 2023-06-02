//! RocksDB Backend
use bytes::{BufMut, Bytes, BytesMut};
use log::{debug, info, warn};
use rocksdb::{
	BlockBasedOptions, ColumnFamily, Options, ReadOptions, SliceTransform, WriteBatch, DB,
};
use std::fmt::Display;
use std::sync::Arc;

use crate::backend::{InnerWorkQueue, WorkQueueBackend};
use crate::error::{QError, QResult};
use crate::jobs::{Job, JobStatus};
use crate::unix_millis;

const CF_WAIT: &str = "w";
const CF_RUN: &str = "r";
const CF_FIN: &str = "f";
const CF_TIME_EXP: &str = "te";
const CF_TIME_SCHED: &str = "ts";

#[derive(Debug, Clone)]
pub struct RocksBackend {
	db: Arc<DB>,
}

impl From<rocksdb::Error> for QError {
	fn from(error: rocksdb::Error) -> Self {
		QError::StoreError(error.into_string())
	}
}

/// RocksDB backend for Bluefan work queues.
impl RocksBackend {
	pub fn new(path: &str) -> Self {
		let mut opts = Options::default();
		opts.create_if_missing(true);
		opts.create_missing_column_families(true);
		opts.increase_parallelism(std::thread::available_parallelism().unwrap().get() as i32);

		let cfs = DB::list_cf(&opts, path).ok();
		let db = match cfs {
			Some(cfs) => DB::open_cf(&opts, path, cfs).expect("open db"),
			None => {
				let mut db =
					DB::open_cf(&opts, path, [CF_TIME_SCHED, CF_TIME_EXP]).expect("open db");

				let mut tableopts = BlockBasedOptions::default();
				tableopts.set_index_type(rocksdb::BlockBasedIndexType::HashSearch);

				let mut prefixopts = Options::default();
				// 4-bytes queue hash prefix
				prefixopts.set_prefix_extractor(SliceTransform::create_fixed_prefix(4));
				prefixopts.set_block_based_table_factory(&tableopts);
				prefixopts.set_memtable_prefix_bloom_ratio(0.2);

				db.create_cf(CF_WAIT, &prefixopts).expect("create wait cf");
				db.create_cf(CF_RUN, &prefixopts).expect("create run cf");
				db.create_cf(CF_FIN, &prefixopts).expect("create fin cf");

				db
			},
		};

		info!("RocksDB: open in {}", path);

		Self { db: Arc::new(db) }
	}

	fn get_job(
		&self,
		cf: &ColumnFamily,
		queue_hash: u32,
		job_id: u64,
	) -> QResult<Option<Job>> {
		match self.db.get_cf_opt(cf, JobKey::bytes_from(queue_hash, job_id), &self.readopts())
		{
			Ok(Some(bytes)) => Ok(Some(self.deserialize(&bytes))),
			_ => Ok(None),
		}
	}

	fn readopts(&self) -> ReadOptions {
		let mut read_opts = ReadOptions::default();
		read_opts.set_verify_checksums(false);
		read_opts
	}

	fn deserialize(&self, bytes: &[u8]) -> Job {
		bincode::deserialize_from(bytes).expect("deserialize job")
	}

	fn serialize(&self, job: &Job) -> Vec<u8> {
		bincode::serialize(job).expect("serialize job")
	}

	fn cf_fin(&self) -> &ColumnFamily {
		self.db.cf_handle(CF_FIN).unwrap()
	}

	fn cf_wait(&self) -> &ColumnFamily {
		self.db.cf_handle(CF_WAIT).unwrap()
	}

	fn cf_run(&self) -> &ColumnFamily {
		self.db.cf_handle(CF_RUN).unwrap()
	}

	fn cf_time_sched(&self) -> &ColumnFamily {
		self.db.cf_handle(CF_TIME_SCHED).unwrap()
	}

	fn cf_time_exp(&self) -> &ColumnFamily {
		self.db.cf_handle(CF_TIME_EXP).unwrap()
	}
}

impl WorkQueueBackend for RocksBackend {
	fn start<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<()> {
		let cf = self.cf_wait();

		let num_keys = self
			.db
			.property_int_value_cf(cf, rocksdb::properties::ESTIMATE_NUM_KEYS)?
			.unwrap();

		// TODO: improve with bulk loading?
		if num_keys > 0 {
			info!("RocksDB: recover waiting jobs [wait_keys={}]", num_keys);

			let ready_jobs = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
			for (key, bytes) in ready_jobs.flatten() {
				q.inner_put(JobKey::queue_hash_from_key(&key), self.deserialize(&bytes))?;
			}
		} else {
			info!("RocksDB: no waiting jobs to recover")
		}

		Ok(())
	}

	fn put_job(&self, queue_hash: u32, job: &Job) -> QResult<()> {
		let job_id = job.id.unwrap();
		let job_key = &JobKey::new(queue_hash, job_id);

		if job.options.delay > 0 {
			let tkey = TimeKey::new(job.state.delay_ts, job_key);
			let mut batch = WriteBatch::default();

			batch.put_cf(self.cf_time_sched(), tkey, []);
			batch.put_cf(self.cf_wait(), job_key, self.serialize(job));

			self.db.write(batch)?;
		} else {
			self.db.put_cf(self.cf_wait(), job_key, self.serialize(job))?;
		}

		Ok(())
	}

	fn claim_job(&self, queue_hash: u32, job: &Job) -> QResult<()> {
		let job_id = job.id.unwrap();
		let job_key = &JobKey::new(queue_hash, job_id);

		let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();
		batch.delete_cf(self.cf_wait(), job_key);
		batch.put_cf(self.cf_run(), job_key, self.serialize(job));

		if job.options.ttr > 0 {
			batch.put_cf(self.cf_time_exp(), TimeKey::new(job.state.ttr_ts, job_key), CF_RUN);
		}

		self.db.write(batch)?;

		Ok(())
	}

	fn release_job(&self, queue_hash: u32, job_id: u64) -> QResult<Job> {
		let job_key = &JobKey::new(queue_hash, job_id);

		if self.db.key_may_exist_cf(self.cf_run(), job_key) {
			if let Ok(Some(mut job)) = self.get_job_running(queue_hash, job_id) {
				let mut batch = WriteBatch::default();

				job.state.status = JobStatus::WAIT;

				if job.state.ttr_ts > 0 {
					batch.delete_cf(
						self.cf_time_exp(),
						TimeKey::new(job.state.ttr_ts, job_key),
					);
				}

				// TODO do we want to respect delay on release?
				batch.delete_cf(self.cf_run(), job_key);
				batch.put_cf(self.cf_wait(), job_key, self.serialize(&job));

				self.db.write(batch)?;

				return Ok(job);
			}
		}

		Err(QError::QueueError(String::from("unable to relase job")))
	}

	fn delete_job(&self, queue_hash: u32, job_id: u64) -> QResult<()> {
		let job_key = &JobKey::new(queue_hash, job_id);

		if self.db.key_may_exist_cf(self.cf_run(), job_key) {
			if let Ok(Some(job)) = self.get_job_running(queue_hash, job_id) {
				let mut batch = WriteBatch::default();
				batch.delete_cf(self.cf_run(), job_key);
				if job.state.ttr_ts > 0 {
					batch.delete_cf(
						self.cf_time_exp(),
						TimeKey::new(job.state.ttr_ts, job_key),
					);
				}
				self.db.write(batch)?;
			}
		} else if self.db.key_may_exist_cf(self.cf_wait(), job_key) {
			if let Ok(Some(job)) = self.get_job_waiting(queue_hash, job_id) {
				let mut batch = WriteBatch::default();
				batch.delete_cf(self.cf_wait(), job_key);
				if job.state.delay_ts > 0 {
					batch.delete_cf(
						self.cf_time_sched(),
						TimeKey::new(job.state.delay_ts, job_key),
					);
				}
				self.db.write(batch)?;
			}
		} else if self.db.key_may_exist_cf(self.cf_fin(), job_key) {
			if let Ok(Some(job)) = self.get_job_completed(queue_hash, job_id) {
				let mut batch = WriteBatch::default();
				batch.delete_cf(self.cf_fin(), job_key);
				if job.state.retention_ts > 0 {
					batch.delete_cf(
						self.cf_time_exp(),
						TimeKey::new(job.state.retention_ts, job_key),
					);
				} else {
					warn!("Job completed in store without retention: {:?}", job_id)
				}
				self.db.write(batch)?;
			}
		}

		Ok(())
	}

	fn sched_tick<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<Option<u64>> {
		let mut delays = self.db.iterator_cf_opt(
			self.cf_time_sched(),
			self.readopts(),
			rocksdb::IteratorMode::Start,
		);
		let now = unix_millis();

		while let Some(Ok((key, _))) = delays.next() {
			let tkey = TimeKey::from(&key);

			if tkey.time > now {
				return Ok(Some(tkey.time - now));
			}

			let job_key = &tkey.job_key;

			if let Ok(Some(job)) = self.get_job_waiting(job_key.queue_hash, job_key.job_id) {
				q.inner_put(job_key.queue_hash, job)?;
			} else {
				warn!("Scheduled job not found in wait area: {}", tkey);
			}
		}

		Ok(None)
	}

	fn exp_tick<Q: InnerWorkQueue>(&self, q: &Q) -> QResult<Option<u64>> {
		let cf_exp = self.cf_time_exp();

		let mut expirations =
			self.db.iterator_cf_opt(cf_exp, self.readopts(), rocksdb::IteratorMode::Start);
		let now = unix_millis();

		while let Some(Ok((key, bytes))) = expirations.next() {
			let tkey = TimeKey::from(&key);
			if tkey.time > now {
				debug!("tick next exp {}", tkey.time - now);
				return Ok(Some(tkey.time - now));
			}

			let job_key = &tkey.job_key;
			let target_cf = std::str::from_utf8(&bytes).unwrap();

			if CF_RUN == target_cf {
				if let Ok(Some(mut job)) =
					self.get_job_running(job_key.queue_hash, job_key.job_id)
				{
					debug!("ttr evict {:?}", job_key);

					let mut batch = WriteBatch::default();
					batch.delete_cf(self.cf_run(), job_key);
					batch.delete_cf(cf_exp, job_key);
					self.db.write(batch)?;

					if job.options.max_retries > job.state.retries_count {
						job.state.retries_count += 1;
						// TODO with delay
						q.inner_put(job_key.queue_hash, job)?;
					} else {
						// job reached max retries
						job.state.status = JobStatus::ERR;
						job.state.outcome = Some("Reached max retries".into());
						self.complete_job(job_key.queue_hash, &job)?;
					}
				}
			} else if CF_FIN == target_cf && self.db.key_may_exist_cf(self.cf_fin(), job_key) {
				debug!("retention evict {:?}", job_key);

				let mut batch = WriteBatch::default();
				batch.delete_cf(self.cf_fin(), job_key);
				batch.delete_cf(cf_exp, key);
				self.db.write(batch)?;
			}
		}
		Ok(None)
	}

	fn peek_job(&self, queue_hash: u32) -> QResult<Vec<Job>> {
		let mut readopts = ReadOptions::default();
		readopts.set_iterate_lower_bound(queue_hash.to_be_bytes());
		readopts.set_prefix_same_as_start(true);

		let items =
			self.db.iterator_cf_opt(self.cf_wait(), readopts, rocksdb::IteratorMode::Start);

		let mut keys = Vec::new();
		for (_, bytes) in items.take(100).flatten() {
			keys.push(self.deserialize(&bytes))
		}

		Ok(keys)
	}

	fn get_job_running(&self, queue_hash: u32, job_id: u64) -> QResult<Option<Job>> {
		self.get_job(self.cf_run(), queue_hash, job_id)
	}

	fn get_job_completed(&self, queue_hash: u32, job_id: u64) -> QResult<Option<Job>> {
		self.get_job(self.cf_fin(), queue_hash, job_id)
	}

	fn get_job_waiting(&self, queue_hash: u32, job_id: u64) -> QResult<Option<Job>> {
		self.get_job(self.cf_wait(), queue_hash, job_id)
	}

	fn touch_job(&self, queue_hash: u32, job_id: u64) -> QResult<()> {
		if let Ok(Some(mut job)) = self.get_job_running(queue_hash, job_id) {
			if job.state.ttr_ts > 0 {
				let cf_run = self.cf_run();
				let cf_exp = self.cf_time_exp();

				let job_key = &JobKey::new(queue_hash, job_id);
				let prev_tkey = TimeKey::new(job.state.ttr_ts, job_key);
				let ttr = unix_millis() + job.options.ttr as u64;
				let tkey = TimeKey::new(ttr, job_key);

				let mut batch = WriteBatch::default();
				batch.delete_cf(cf_exp, prev_tkey);
				batch.put_cf(cf_exp, tkey, CF_RUN);

				job.state.ttr_ts = ttr;
				batch.delete_cf(cf_run, job_key);
				batch.put_cf(cf_run, job_key, self.serialize(&job));

				self.db.write(batch)?;
			}
		}

		Ok(())
	}

	fn stop(&self) {
		info!("RocksDB: stop backend");

		self.db.flush().expect("flush to disk");

		/*
		// Create checkpoint
		let cp_path = &format!("{}/cp", self.db.path().to_str().unwrap());
		info!("Checkpoint in: {}", cp_path);
		let cp = Checkpoint::new(&self.db).unwrap();
		cp.create_checkpoint(cp_path).unwrap();
		*/

		info!("RocksDB: end");
	}

	fn update_job(&self, queue_hash: u32, job: &Job) -> QResult<()> {
		let job_id = job.id.unwrap();
		let job_key = &JobKey::new(queue_hash, job_id);
		let db_key = job_key;

		let mut batch = WriteBatch::default();
		batch.delete_cf(self.cf_run(), db_key);
		batch.put_cf(self.cf_run(), db_key, self.serialize(job));
		self.db.write(batch)?;

		self.touch_job(queue_hash, job_id)
	}

	fn complete_job(&self, queue_hash: u32, job: &Job) -> QResult<()> {
		let job_key = &JobKey::new(queue_hash, job.id.unwrap());

		let mut batch = WriteBatch::default();
		batch.delete_cf(self.cf_run(), job_key);
		if job.state.ttr_ts > 0 {
			batch.delete_cf(self.cf_time_exp(), TimeKey::new(job.state.ttr_ts, job_key));
		}
		if job.options.retention > 0 {
			let mut job = job.clone();
			job.state.retention_ts = unix_millis() + job.options.retention as u64;

			batch.put_cf(
				self.cf_time_exp(),
				TimeKey::new(job.state.retention_ts, job_key),
				CF_FIN,
			);
			batch.put_cf(self.cf_fin(), job_key, self.serialize(&job));
		}
		self.db.write(batch)?;

		Ok(())
	}
}

#[derive(Debug, Clone)]
struct JobKey {
	/// Hash of the queue name
	queue_hash: u32,
	/// Job identifier
	job_id: u64,
	///
	bytes: Bytes,
}

impl JobKey {
	fn new(queue_hash: u32, job_id: u64) -> Self {
		Self { queue_hash, job_id, bytes: JobKey::bytes_from(queue_hash, job_id) }
	}

	fn bytes_from(queue_hash: u32, job_id: u64) -> Bytes {
		let mut bytes = BytesMut::with_capacity(12);
		bytes.put_u32(queue_hash);
		bytes.put_u64(job_id);
		bytes.into()
	}

	fn queue_hash_from_key(key: &[u8]) -> u32 {
		u32::from_be_bytes(key[..4].try_into().expect("queue name hash"))
	}
}

impl From<&[u8]> for JobKey {
	fn from(key_bytes: &[u8]) -> Self {
		let queue_hash =
			u32::from_be_bytes(key_bytes.get(..4).expect("queue name").try_into().unwrap());
		let job_id = u64::from_be_bytes(
			key_bytes.get(4..).expect("job key in the rest").try_into().unwrap(),
		);

		Self { queue_hash, job_id, bytes: Bytes::from(key_bytes.to_vec()) }
	}
}

impl AsRef<[u8]> for JobKey {
	fn as_ref(&self) -> &[u8] {
		&self.bytes
	}
}

#[derive(Debug, Clone)]
struct TimeKey {
	/// UNIX epoch millis segment
	time: u64,
	/// Job key
	job_key: JobKey,
	///
	bytes: Bytes,
}

impl TimeKey {
	fn from(key: &[u8]) -> Self {
		let time = u64::from_be_bytes(
			key.get(..8).expect("timestamp in the first 8 octets").try_into().unwrap(),
		);
		let job_key = &JobKey::from(key.get(8..).unwrap());

		Self::new(time, job_key)
	}

	fn bytes_from(time: u64, job_key: &JobKey) -> Bytes {
		let mut bytes = BytesMut::with_capacity(20);
		bytes.put_u64(time);
		bytes.put_u32(job_key.queue_hash);
		bytes.put_u64(job_key.job_id);
		bytes.into()
	}

	fn to_bytes(&self) -> &Bytes {
		&self.bytes
	}

	fn new(time: u64, job_key: &JobKey) -> TimeKey {
		Self { time, job_key: job_key.clone(), bytes: TimeKey::bytes_from(time, job_key) }
	}
}

impl AsRef<[u8]> for TimeKey {
	fn as_ref(&self) -> &[u8] {
		self.to_bytes()
	}
}

impl Display for TimeKey {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		f.write_fmt(format_args!("TimeKey {}", self))
	}
}
