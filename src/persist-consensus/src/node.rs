// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Raft node setup — wires together the Raft instance, storage, and network.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{Config, Raft};

use crate::network::GrpcNetworkFactory;
use crate::raft_types::{NodeInfo, TypeConfig};
use crate::state_machine::StateMachineStore;
use crate::storage::Storage;

/// A Raft node bundling the openraft instance with shared access to the state machine.
pub struct RaftNode {
    pub raft: Raft<TypeConfig>,
    pub state_machine: StateMachineStore,
}

impl RaftNode {
    /// Creates a new Raft node with in-memory log and state machine storage.
    pub async fn new(node_id: u64) -> anyhow::Result<Self> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );

        let (storage, sm_store) = Storage::new();
        let (log_store, state_machine) = Adaptor::new(storage);
        let network = GrpcNetworkFactory;

        let raft =
            Raft::new(node_id, config, network, log_store, state_machine).await?;

        Ok(Self {
            raft,
            state_machine: sm_store,
        })
    }

    /// Bootstrap a single-node or multi-node cluster.
    ///
    /// Must only be called once, on the initial startup of the cluster.
    pub async fn bootstrap(
        &self,
        members: BTreeMap<u64, NodeInfo>,
    ) -> anyhow::Result<()> {
        self.raft.initialize(members).await?;
        Ok(())
    }
}
