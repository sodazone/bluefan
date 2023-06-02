# Bluefan Queue Services

Bluefan Queue Services provide fast and reliable work queues for distributed job processing.

## Features

- First in, first out (FIFO) job delivery for concurrent competing consumers
- Persistence of jobs for system restarts or failures
- Automatic job retry and time limit for execution
- Support for at-most-once and at-least-once work processing semantics

## Usage

> This crate is intended to be exposed by a networked server.
> See [gRPC Server](../rpc).

```rust
use bluefan_queue::{
  jobs::Job,
  RocksWorkQueueService
};

let work_queue = RocksWorkQueueService::new("./.db").queue();

// Producer puts a job
work_queue.put("q1", &Job::default());

// Consumer leases the job
if let Ok(Some(job)) = work_queue.lease("q1") {
  println!("{:?}", job);
}
```

