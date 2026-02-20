// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Binary entry point for the persist-consensus service.

use std::net::SocketAddr;

use clap::Parser;
use mz_ore::error::ErrorExt;

/// Raft-backed consensus service for Materialize persist.
#[derive(Debug, Parser)]
#[clap(about = "Persist consensus service", long_about = None)]
struct Args {
    /// Unique node identifier within the Raft cluster.
    #[clap(long, env = "NODE_ID")]
    node_id: u64,

    /// Address to listen on for the external Consensus API (gRPC).
    #[clap(long, default_value = "0.0.0.0:6880", env = "API_LISTEN_ADDR")]
    api_listen_addr: SocketAddr,

    /// Address to listen on for internal Raft node-to-node RPCs (gRPC).
    #[clap(long, default_value = "0.0.0.0:6881", env = "RAFT_LISTEN_ADDR")]
    raft_listen_addr: SocketAddr,

    /// Peer nodes in the format "node_id:raft_addr:api_addr".
    /// Can be specified multiple times.
    #[clap(long)]
    peer: Vec<String>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");

    let res = runtime.block_on(mz_persist_consensus::run(
        mz_persist_consensus::RunArgs {
            node_id: args.node_id,
            api_listen_addr: args.api_listen_addr,
            raft_listen_addr: args.raft_listen_addr,
            peers: args.peer,
        },
    ));

    if let Err(err) = res {
        eprintln!("persist-consensus: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
}
