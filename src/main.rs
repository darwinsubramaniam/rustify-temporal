//! rustify-temporal: a Temporal-compatible workflow server written in Rust.
//!
//! The official Temporal SDKs (Python, Go, Java, TypeScript, .NET, Ruby, Rust) connect to it over
//! gRPC exactly as they would to the Temporal server.

#![allow(deprecated)]

pub mod proto {
    #![allow(clippy::all, unused, non_camel_case_types, rustdoc::all)]
    include!(concat!(env!("OUT_DIR"), "/temporal_protos.rs"));
}

mod engine;
mod service;
mod state;
mod store;
mod util;

use std::net::SocketAddr;

use std::sync::Arc;
use tonic::codec::CompressionEncoding;

use proto::temporal::api::operatorservice::v1::operator_service_server::{OperatorService, OperatorServiceServer};
use proto::temporal::api::workflowservice::v1::workflow_service_server::WorkflowServiceServer;
use proto::temporal::server::api::adminservice::v1::admin_service_server::{AdminService, AdminServiceServer};

const USAGE: &str = "rustify-temporal - a Temporal-compatible workflow server written in Rust

USAGE:
    rustify-temporal [OPTIONS]

OPTIONS:
    --ip <ADDR>        Address to listen on (default 127.0.0.1)
    --port <PORT>      gRPC port the SDKs connect to (default 7233)
    --db <PATH>        SQLite database file (default temporal.db; ':memory:' keeps nothing)
    --namespace <NS>   Extra namespace to create at startup (repeatable)
    -h, --help         Show this help

Logging follows RUST_LOG (default: info).";

struct Args {
    ip: String,
    port: u16,
    db: String,
    namespaces: Vec<String>,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut a = Args {
        ip: "127.0.0.1".into(),
        port: 7233,
        db: "temporal.db".into(),
        namespaces: vec![],
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| anyhow::anyhow!("{name} needs a value"));
        match arg.as_str() {
            "--ip" => a.ip = val("--ip")?,
            "--port" => a.port = val("--port")?.parse()?,
            "--db" => a.db = val("--db")?,
            "--namespace" => a.namespaces.push(val("--namespace")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown option {other}\n\n{USAGE}"),
        }
    }
    Ok(a)
}

/// The operator service only exists so SDK test helpers that probe it get clean "unimplemented" answers.
struct Operator;
#[tonic::async_trait]
impl OperatorService for Operator {}

/// The Go server's internal admin API (tdbg, `temporal operator` admin commands): served with its
/// exact contract so clients get clean "unimplemented" answers until each operation is built.
struct Admin;
#[tonic::async_trait]
impl AdminService for Admin {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = parse_args()?;
    let store = store::Store::open(&args.db)?;
    let engine = engine::Engine::open(store)?;
    for ns in &args.namespaces {
        let r = engine.op(|i| {
            if i.ns_id(ns).is_ok() {
                return Ok(());
            }
            i.register_namespace(util::wsv1::RegisterNamespaceRequest {
                namespace: ns.clone(),
                workflow_execution_retention_period: Some(util::dur_from_ms(engine::namespace::DEFAULT_RETENTION_MS)),
                ..Default::default()
            })
        });
        if let Err(e) = r {
            anyhow::bail!("could not create namespace {ns}: {}", e.message());
        }
    }
    tokio::spawn(Arc::clone(&engine).run_timers());

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<WorkflowServiceServer<service::Svc>>()
        .await;

    let addr: SocketAddr = format!("{}:{}", args.ip, args.port).parse()?;
    tracing::info!(
        "rustify-temporal {} listening on {addr} (database: {})",
        service::SERVER_VERSION,
        args.db
    );
    let svc = WorkflowServiceServer::new(service::Svc { engine: engine.clone() })
        .max_decoding_message_size(128 * 1024 * 1024)
        .max_encoding_message_size(128 * 1024 * 1024)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
    tonic::transport::Server::builder()
        // Send small gRPC frames immediately (Go servers do this by default); without it, Nagle plus
        // delayed ACKs add ~40-100 ms to every long-poll answer.
        .tcp_nodelay(true)
        .add_service(health_service)
        .add_service(svc)
        .add_service(OperatorServiceServer::new(Operator))
        .add_service(AdminServiceServer::new(Admin))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}
