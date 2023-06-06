//! # Bluefan gRPC Server
//!
use std::net::SocketAddr;
use std::path::PathBuf;

use bluefan_queue::RocksWorkQueueService;
use clap::{Parser, ValueHint};
use log::info;
use tonic::transport::Server;

mod protocol;
use crate::protocol::bluefan_rpc::queue_server::QueueServer;
use crate::protocol::QueueRpc;

#[derive(Parser)]
#[command(name = "Bluefan RPC Server")]
#[command(author = "soda <projects@soda.zone>")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Bluefan RPC Work Queue Server", long_about = None)]
#[command(next_line_help = true)]
struct Cli {
	/// Directory for the database
	#[arg(short, long, default_value = "./_db", value_name = "DIR", value_hint = ValueHint::DirPath)]
	db: PathBuf,

	/// Socket address to listen
	#[arg(short, long, default_value = "[::1]:50051", value_name = "IP:PORT")]
	addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();

	let cli = Cli::parse();

	info!("gRPC server listening on {}", cli.addr);

	let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
	health_reporter.set_serving::<QueueServer<QueueRpc>>().await;

	let queue_service = RocksWorkQueueService::new(cli.db.to_str().unwrap());
	let rpc = QueueRpc::new(queue_service);

	Server::builder()
		.add_service(health_service)
		.add_service(QueueServer::new(rpc))
		.serve(cli.addr)
		.await?;

	Ok(())
}
