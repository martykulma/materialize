// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A standalone Raft-backed consensus service for Materialize persist.
//!
//! This crate implements the `Consensus` trait from `mz_persist::location` as a
//! gRPC service backed by an [openraft](https://github.com/databendlabs/openraft)
//! Raft cluster. It provides:
//!
//! - **External API** (`PersistConsensusService`): gRPC service for `head`, `compare_and_set`,
//!   `scan`, `truncate`, and `list_keys` operations.
//! - **Internal Raft RPCs** (`RaftService`): gRPC service for Raft node-to-node communication.
//! - **Client** (`GrpcConsensusClient`): Implements the `Consensus` trait over gRPC.

pub mod client;
pub mod generated;
pub mod network;
pub mod node;
pub mod raft_types;
pub mod server;
pub mod state_machine;
pub mod storage;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tracing::info;

use crate::generated::raft::raft_service_server::RaftServiceServer;
use crate::generated::service::persist_consensus_service_server::PersistConsensusServiceServer;
use crate::node::RaftNode;
use crate::raft_types::NodeInfo;
use crate::server::{ConsensusServer, RaftRpcServer};

/// Arguments for starting the persist-consensus service.
pub struct RunArgs {
    pub node_id: u64,
    pub api_listen_addr: SocketAddr,
    pub raft_listen_addr: SocketAddr,
    pub peers: Vec<String>,
}

/// Main entry point: starts the Raft node and both gRPC servers.
pub async fn run(args: RunArgs) -> anyhow::Result<()> {
    let node = RaftNode::new(args.node_id).await?;

    // Build peer membership map from CLI args.
    // Format: "node_id:raft_addr:api_addr" e.g. "1:127.0.0.1:6881:127.0.0.1:6880"
    let mut members = BTreeMap::new();
    members.insert(
        args.node_id,
        NodeInfo {
            raft_addr: args.raft_listen_addr.to_string(),
            api_addr: args.api_listen_addr.to_string(),
        },
    );
    for peer in &args.peers {
        let parts: Vec<&str> = peer.splitn(3, ':').collect();
        if parts.len() == 3 {
            let peer_id: u64 = parts[0].parse()?;
            let peer_raft_addr = parts[1].to_string();
            let peer_api_addr = parts[2].to_string();
            members.insert(
                peer_id,
                NodeInfo {
                    raft_addr: peer_raft_addr,
                    api_addr: peer_api_addr,
                },
            );
        }
    }

    // Bootstrap cluster on first run. If already initialized, this is a no-op error.
    if let Err(e) = node.bootstrap(members).await {
        info!("Raft bootstrap (may be expected if already initialized): {e}");
    }

    let raft = node.raft.clone();
    let state_machine = node.state_machine.clone();

    // Start both gRPC servers concurrently.
    let api_server = tonic::transport::Server::builder()
        .add_service(PersistConsensusServiceServer::new(ConsensusServer {
            raft: raft.clone(),
            state_machine: Arc::new(state_machine),
        }))
        .serve(args.api_listen_addr);

    let raft_server = tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftRpcServer { raft }))
        .serve(args.raft_listen_addr);

    info!(
        "persist-consensus node {} listening: api={}, raft={}",
        args.node_id, args.api_listen_addr, args.raft_listen_addr
    );

    tokio::try_join!(api_server, raft_server)?;

    Ok(())
}
