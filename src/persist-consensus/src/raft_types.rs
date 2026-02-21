// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! OpenRaft type configuration for persist-consensus.

use std::io::Cursor;

use serde::{Deserialize, Serialize};

/// A request to modify consensus state, proposed through Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusRequest {
    CompareAndSet {
        key: String,
        expected: Option<u64>,
        new_seqno: u64,
        new_data: Vec<u8>,
    },
    Truncate {
        key: String,
        seqno: u64,
    },
    /// A batch of requests applied atomically in order.
    /// Used by the write batcher to coalesce concurrent proposals.
    Batch(Vec<ConsensusRequest>),
}

/// The response from applying a consensus request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusResponse {
    CompareAndSet {
        committed: bool,
    },
    Truncate {
        deleted: Option<usize>,
    },
    Error {
        message: String,
    },
    /// Responses for a batch of requests, in the same order as the input.
    Batch(Vec<ConsensusResponse>),
}

/// Information about a Raft cluster node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NodeInfo {
    pub raft_addr: String,
    pub api_addr: String,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft={}, api={}", self.raft_addr, self.api_addr)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ConsensusRequest,
        R = ConsensusResponse,
        Node = NodeInfo,
        NodeId = u64,
);
