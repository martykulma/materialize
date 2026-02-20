// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC-based Raft network implementation.
//!
//! Connects to peer nodes via the `RaftService` gRPC client, serializing
//! openraft request types via serde JSON into opaque `ProtoRaftRequest` payloads.

use std::future::Future;

use openraft::error::{
    Fatal, InstallSnapshotError, RPCError, RaftError, ReplicationClosed, StreamingError,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{Snapshot, Vote};

use crate::generated::raft::raft_service_client::RaftServiceClient;
use crate::generated::raft::ProtoRaftRequest;
use crate::raft_types::{NodeInfo, TypeConfig};

/// Factory that creates gRPC network connections to peer Raft nodes.
#[derive(Debug, Clone, Default)]
pub struct GrpcNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(&mut self, _target: u64, node: &NodeInfo) -> Self::Network {
        GrpcNetwork {
            raft_addr: node.raft_addr.clone(),
        }
    }
}

/// A gRPC network connection to a single peer Raft node.
#[derive(Debug)]
pub struct GrpcNetwork {
    raft_addr: String,
}

impl GrpcNetwork {
    async fn connect(
        &self,
    ) -> Result<
        RaftServiceClient<tonic::transport::Channel>,
        RPCError<u64, NodeInfo, RaftError<u64>>,
    > {
        let addr = format!("http://{}", self.raft_addr);
        RaftServiceClient::connect(addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

impl RaftNetwork<TypeConfig> for GrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, NodeInfo, RaftError<u64>>> {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let mut client = self.connect().await?;
        let response = client
            .append_entries(ProtoRaftRequest { payload })
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let resp: AppendEntriesResponse<u64> =
            serde_json::from_slice(&response.into_inner().payload)
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, NodeInfo, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let addr = format!("http://{}", self.raft_addr);
        let mut client = RaftServiceClient::connect(addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let response = client
            .install_snapshot(ProtoRaftRequest { payload })
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let resp: InstallSnapshotResponse<u64> =
            serde_json::from_slice(&response.into_inner().payload)
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(resp)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        // Serialize the snapshot: meta + data bytes.
        let snapshot_data = snapshot.snapshot.into_inner();
        let transfer = serde_json::json!({
            "vote": vote,
            "meta": snapshot.meta,
            "snapshot_data": snapshot_data,
        });
        let payload = serde_json::to_vec(&transfer)
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        let addr = format!("http://{}", self.raft_addr);
        let mut client = RaftServiceClient::connect(addr)
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        let response = client
            .install_snapshot(ProtoRaftRequest { payload })
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        let resp: SnapshotResponse<u64> = serde_json::from_slice(&response.into_inner().payload)
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        Ok(resp)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, NodeInfo, RaftError<u64>>> {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let mut client = self.connect().await?;
        let response = client
            .vote(ProtoRaftRequest { payload })
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let resp: VoteResponse<u64> = serde_json::from_slice(&response.into_inner().payload)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(resp)
    }
}
