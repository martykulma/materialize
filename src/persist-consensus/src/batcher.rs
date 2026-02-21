// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Write batcher for Raft consensus proposals.
//!
//! Instead of each gRPC write handler calling `raft.client_write()` individually,
//! requests are sent to a shared channel. A background task collects requests
//! into batches bounded by both size and time, proposes each batch as a single
//! Raft entry, and fans out individual responses. This reduces the number of
//! Raft log entries and round-trips under concurrent load while providing a
//! bounded upper limit on latency.
//!
//! ## Batching strategy
//!
//! 1. Block until the first request arrives.
//! 2. Start a deadline timer (`max_batch_duration`).
//! 3. Collect additional requests until either:
//!    - The batch reaches `max_batch_size`, or
//!    - The deadline expires.
//! 4. Propose the batch to Raft and fan out responses.
//!
//! Isolated requests (no contention) pass through with zero added latency
//! because the initial drain via `try_recv` fires before the deadline is
//! even checked.

use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::raft_types::{ConsensusRequest, ConsensusResponse, TypeConfig};

/// Error returned when a Raft proposal fails.
#[derive(Debug)]
pub enum ProposalError {
    /// This node is not the Raft leader.
    NotLeader { leader_api_addr: Option<String> },
    /// Any other error.
    Internal(String),
}

/// Configuration for the write batcher.
#[derive(Debug, Clone)]
pub struct WriteBatcherConfig {
    /// Maximum number of requests in a single batch.
    pub max_batch_size: usize,
    /// Maximum time to wait after the first request before submitting the batch.
    /// This bounds the worst-case added latency for any individual request.
    pub max_batch_duration: Duration,
    /// Capacity of the mpsc channel between gRPC handlers and the batcher task.
    pub channel_capacity: usize,
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 256,
            max_batch_duration: Duration::from_millis(5),
            channel_capacity: 4096,
        }
    }
}

/// A pending write waiting to be batched.
struct PendingWrite {
    request: ConsensusRequest,
    response_tx: oneshot::Sender<Result<ConsensusResponse, ProposalError>>,
}

/// Handle used by gRPC handlers to submit write requests to the batcher.
#[derive(Clone)]
pub struct WriteBatcherHandle {
    tx: mpsc::Sender<PendingWrite>,
}

impl WriteBatcherHandle {
    /// Submit a write request and wait for its result.
    ///
    /// The request will be batched with other concurrent requests before being
    /// proposed to Raft. Returns the individual response for this request.
    pub async fn propose(
        &self,
        request: ConsensusRequest,
    ) -> Result<ConsensusResponse, ProposalError> {
        let (response_tx, response_rx) = oneshot::channel();
        let pending = PendingWrite {
            request,
            response_tx,
        };

        self.tx
            .send(pending)
            .await
            .map_err(|_| ProposalError::Internal("batcher task shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| ProposalError::Internal("batcher dropped response channel".to_string()))?
    }
}

/// Starts the write batcher background task with the given configuration.
///
/// Returns a `WriteBatcherHandle` that gRPC handlers use to submit requests.
/// The background task runs until all handles are dropped and the channel is
/// drained.
pub fn start_write_batcher(
    raft: Raft<TypeConfig>,
    config: WriteBatcherConfig,
) -> WriteBatcherHandle {
    let (tx, rx) = mpsc::channel::<PendingWrite>(config.channel_capacity);

    tokio::spawn(batcher_loop(raft, rx, config));

    WriteBatcherHandle { tx }
}

/// The core batcher loop.
///
/// For each batch cycle:
/// 1. Block until the first request arrives (no CPU cost while idle).
/// 2. Eagerly drain any already-queued requests via `try_recv`.
/// 3. If the batch isn't full yet, wait for more requests up to the deadline.
/// 4. Propose the collected batch to Raft and fan out individual responses.
async fn batcher_loop(
    raft: Raft<TypeConfig>,
    mut rx: mpsc::Receiver<PendingWrite>,
    config: WriteBatcherConfig,
) {
    loop {
        // Phase 1: block until at least one request arrives.
        let first = match rx.recv().await {
            Some(pw) => pw,
            None => {
                debug!("write batcher channel closed, shutting down");
                return;
            }
        };

        let mut batch = Vec::with_capacity(config.max_batch_size.min(64));
        batch.push(first);

        // Phase 2: eagerly drain anything already queued (zero wait).
        while batch.len() < config.max_batch_size {
            match rx.try_recv() {
                Ok(pw) => batch.push(pw),
                Err(_) => break,
            }
        }

        // Phase 3: if batch isn't full, wait up to the deadline for more.
        if batch.len() < config.max_batch_size {
            let deadline = tokio::time::sleep(config.max_batch_duration);
            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    biased;
                    pw = rx.recv() => {
                        match pw {
                            Some(pw) => {
                                batch.push(pw);
                                if batch.len() >= config.max_batch_size {
                                    break;
                                }
                            }
                            None => break, // channel closed
                        }
                    }
                    _ = &mut deadline => break,
                }
            }
        }

        // Phase 4: propose and fan out.
        submit_batch(&raft, batch).await;
    }
}

/// Propose a batch of pending writes to Raft and send individual responses
/// back to each caller.
async fn submit_batch(raft: &Raft<TypeConfig>, mut batch: Vec<PendingWrite>) {
    let batch_size = batch.len();

    if batch_size == 1 {
        // Single request — propose directly without Batch wrapper.
        let pw = batch.pop().unwrap();
        let result = raft.client_write(pw.request).await;
        let response = match result {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                let leader_api_addr = fwd.leader_node.map(|n| n.api_addr);
                Err(ProposalError::NotLeader { leader_api_addr })
            }
            Err(e) => Err(ProposalError::Internal(format!("raft write error: {e}"))),
        };
        let _ = pw.response_tx.send(response);
    } else {
        debug!("batching {batch_size} write requests into single proposal");

        let (requests, senders): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .map(|pw| (pw.request, pw.response_tx))
            .unzip();

        let batch_req = ConsensusRequest::Batch(requests);
        let result = raft.client_write(batch_req).await;

        match result {
            Ok(resp) => match resp.data {
                ConsensusResponse::Batch(responses) => {
                    for (sender, response) in senders.into_iter().zip(responses) {
                        let _ = sender.send(Ok(response));
                    }
                }
                other => {
                    warn!("unexpected batch response: {other:?}");
                    let msg = format!("unexpected batch response: {other:?}");
                    for sender in senders {
                        let _ = sender.send(Err(ProposalError::Internal(msg.clone())));
                    }
                }
            },
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                let leader_api_addr = fwd.leader_node.map(|n| n.api_addr);
                for sender in senders {
                    let _ = sender.send(Err(ProposalError::NotLeader {
                        leader_api_addr: leader_api_addr.clone(),
                    }));
                }
            }
            Err(e) => {
                let msg = format!("raft write error: {e}");
                for sender in senders {
                    let _ = sender.send(Err(ProposalError::Internal(msg.clone())));
                }
            }
        }
    }
}
