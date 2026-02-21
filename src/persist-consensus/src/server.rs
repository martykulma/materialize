// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tonic gRPC server implementations.
//!
//! Two services:
//! - `ConsensusServer`: external API implementing `PersistConsensusService` (client-facing)
//! - `RaftRpcServer`: internal Raft node-to-node RPCs implementing `RaftService`

use std::io::Cursor;
use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::{Raft, Snapshot, SnapshotMeta};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::generated::raft::raft_service_server::RaftService as RaftServiceTrait;
use crate::generated::raft::{ProtoRaftRequest, ProtoRaftResponse};
use mz_persist_consensus_client::generated::service::persist_consensus_service_server::PersistConsensusService;
use mz_persist_consensus_client::generated::service::{
    CaSRequest, CaSResponse, HeadRequest, HeadResponse, ListKeysRequest, ListKeysResponse,
    ProtoCaSResult, ProtoVersionedData, ScanRequest, ScanResponse, TruncateRequest,
    TruncateResponse,
};
use crate::batcher::WriteBatcherHandle;
use crate::raft_types::{ConsensusRequest, ConsensusResponse, NodeInfo, TypeConfig};
use crate::state_machine::StateMachineStore;

// ============================================================================
// External API — PersistConsensusService
// ============================================================================

/// Implements the external `PersistConsensusService` gRPC API.
///
/// Read operations go directly to the state machine.
/// Write operations are submitted through the [`WriteBatcherHandle`] which
/// batches concurrent proposals into fewer Raft round-trips.
pub struct ConsensusServer {
    pub batcher: WriteBatcherHandle,
    pub state_machine: Arc<StateMachineStore>,
}

#[tonic::async_trait]
impl PersistConsensusService for ConsensusServer {
    async fn head(
        &self,
        request: Request<HeadRequest>,
    ) -> Result<Response<HeadResponse>, Status> {
        let key = &request.into_inner().key;
        let value = self.state_machine.head(key).map(|vd| ProtoVersionedData {
            seqno: vd.seqno.0,
            data: vd.data.to_vec(),
        });
        Ok(Response::new(HeadResponse { value }))
    }

    async fn compare_and_set(
        &self,
        request: Request<CaSRequest>,
    ) -> Result<Response<CaSResponse>, Status> {
        let req = request.into_inner();
        let consensus_req = ConsensusRequest::CompareAndSet {
            key: req.key,
            expected: req.expected,
            new_seqno: req.new_seqno,
            new_data: req.new_data,
        };

        let resp = self
            .batcher
            .propose(consensus_req)
            .await
            .map_err(|e| Status::internal(e))?;

        match resp {
            ConsensusResponse::CompareAndSet { committed } => {
                let result = if committed {
                    ProtoCaSResult::Committed
                } else {
                    ProtoCaSResult::ExpectationMismatch
                };
                Ok(Response::new(CaSResponse {
                    result: result.into(),
                }))
            }
            ConsensusResponse::Error { message } => Err(Status::invalid_argument(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<ScanResponse>, Status> {
        let req = request.into_inner();
        let from = mz_persist::location::SeqNo(req.from);
        let limit = req.limit as usize;
        let values = self
            .state_machine
            .scan(&req.key, from, limit)
            .into_iter()
            .map(|vd| ProtoVersionedData {
                seqno: vd.seqno.0,
                data: vd.data.to_vec(),
            })
            .collect();
        Ok(Response::new(ScanResponse { values }))
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        let req = request.into_inner();
        let consensus_req = ConsensusRequest::Truncate {
            key: req.key,
            seqno: req.seqno,
        };

        let resp = self
            .batcher
            .propose(consensus_req)
            .await
            .map_err(|e| Status::internal(e))?;

        match resp {
            ConsensusResponse::Truncate { deleted } => Ok(Response::new(TruncateResponse {
                deleted: deleted.map(|d| d as u64),
            })),
            ConsensusResponse::Error { message } => Err(Status::invalid_argument(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    type ListKeysStream = ReceiverStream<Result<ListKeysResponse, Status>>;

    async fn list_keys(
        &self,
        _request: Request<ListKeysRequest>,
    ) -> Result<Response<Self::ListKeysStream>, Status> {
        let keys = self.state_machine.list_keys();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            for key in keys {
                if tx.send(Ok(ListKeysResponse { key })).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ============================================================================
// Internal Raft RPCs — RaftService
// ============================================================================

/// Implements the internal `RaftService` gRPC API for Raft node-to-node communication.
pub struct RaftRpcServer {
    pub raft: Raft<TypeConfig>,
}

#[tonic::async_trait]
impl RaftServiceTrait for RaftRpcServer {
    async fn append_entries(
        &self,
        request: Request<ProtoRaftRequest>,
    ) -> Result<Response<ProtoRaftResponse>, Status> {
        let payload = request.into_inner().payload;
        let req: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(&payload)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(ProtoRaftResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<ProtoRaftRequest>,
    ) -> Result<Response<ProtoRaftResponse>, Status> {
        let payload = request.into_inner().payload;
        let transfer: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let vote: openraft::Vote<u64> =
            serde_json::from_value(transfer.get("vote").cloned().unwrap_or_default())
                .map_err(|e| Status::invalid_argument(format!("deserialize vote: {e}")))?;

        let meta: SnapshotMeta<u64, NodeInfo> =
            serde_json::from_value(transfer.get("meta").cloned().unwrap_or_default())
                .map_err(|e| Status::invalid_argument(format!("deserialize meta: {e}")))?;

        let snapshot_data: Vec<u8> =
            serde_json::from_value(transfer.get("snapshot_data").cloned().unwrap_or_default())
                .map_err(|e| Status::invalid_argument(format!("deserialize snapshot_data: {e}")))?;

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        };

        let resp = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("install snapshot error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(ProtoRaftResponse { payload }))
    }

    async fn vote(
        &self,
        request: Request<ProtoRaftRequest>,
    ) -> Result<Response<ProtoRaftResponse>, Status> {
        let payload = request.into_inner().payload;
        let req: VoteRequest<u64> = serde_json::from_slice(&payload)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(ProtoRaftResponse { payload }))
    }
}
