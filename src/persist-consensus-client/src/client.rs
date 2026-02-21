// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Raw gRPC client for the persist-consensus service.
//!
//! This client provides direct access to the gRPC methods without depending on
//! the `mz_persist::location::Consensus` trait. The trait implementation is
//! provided by `mz_persist::raft::RaftConsensus` to avoid circular dependencies.
//!
//! All operations automatically detect "not leader" responses from follower
//! nodes, reconnect to the current leader using the address provided in gRPC
//! metadata, and transparently retry the request.

use std::sync::Arc;

use tonic::transport::Channel;
use tracing::debug;

use crate::generated::service::persist_consensus_service_client::PersistConsensusServiceClient;
use crate::generated::service::{
    CaSRequest, HeadRequest, ListKeysRequest, ScanRequest, TruncateRequest,
};

/// Maximum number of leader-redirect retries for a single request.
const MAX_REDIRECT_RETRIES: usize = 3;

/// Result type for gRPC client operations.
pub type ClientResult<T> = Result<T, tonic::Status>;

/// Versioned data returned by the service.
#[derive(Debug, Clone)]
pub struct VersionedData {
    /// The sequence number.
    pub seqno: u64,
    /// The data payload.
    pub data: Vec<u8>,
}

/// Result of a compare-and-set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaSResult {
    /// The value was committed.
    Committed,
    /// The expected sequence number did not match.
    ExpectationMismatch,
}

/// Internal connection state protected by a read-write lock.
struct ConnectionState {
    client: PersistConsensusServiceClient<Channel>,
    known_addrs: Vec<String>,
}

/// A raw gRPC client for the persist-consensus service.
///
/// Automatically redirects requests to the Raft leader when connected to a
/// follower node. The leader's address is discovered from `x-leader-addr`
/// gRPC metadata returned by follower nodes.
#[derive(Clone)]
pub struct PersistConsensusClient {
    state: Arc<tokio::sync::RwLock<ConnectionState>>,
}

impl std::fmt::Debug for PersistConsensusClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistConsensusClient").finish()
    }
}

impl PersistConsensusClient {
    /// Connect to a persist-consensus server at the given address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = PersistConsensusServiceClient::connect(addr.clone()).await?;
        let state = ConnectionState {
            client,
            known_addrs: vec![addr],
        };
        Ok(Self {
            state: Arc::new(tokio::sync::RwLock::new(state)),
        })
    }

    /// Returns a clone of the current gRPC client.
    async fn get_client(&self) -> PersistConsensusServiceClient<Channel> {
        self.state.read().await.client.clone()
    }

    /// Reconnect to the given leader address, swapping the active client.
    async fn reconnect_to_leader(&self, addr: &str) -> Result<(), tonic::transport::Error> {
        let mut state = self.state.write().await;
        // Another concurrent call may have already reconnected.
        if state.known_addrs[0].as_str() == addr {
            return Ok(());
        }
        debug!(
            leader_addr = addr,
            prev_addr = state.known_addrs[0].as_str(),
            "reconnecting to leader"
        );

        let new_client = PersistConsensusServiceClient::connect(addr.to_string()).await?;
        state.client = new_client;
        if !state.known_addrs.iter().any(|a| a == addr) {
            state.known_addrs.push(addr.to_string());
        }
        Ok(())
    }

    /// Extract the leader address from `x-leader-addr` metadata on an
    /// `UNAVAILABLE` status.
    ///
    /// This returns the address as a URI. Currently, the scheme is always http.
    fn parse_leader_redirect(status: &tonic::Status) -> Option<String> {
        if status.code() != tonic::Code::Unavailable {
            return None;
        }
        Some(format!(
            "http://{}",
            status
                .metadata()
                .get("x-leader-addr")
                .expect("x-leader-addr is set")
                .to_str()
                .expect("able to decode leader address")
        ))
    }

    /// Returns the latest versioned data for the given key.
    ///
    /// Automatically retries on leader redirect (up to [`MAX_REDIRECT_RETRIES`]
    /// attempts).
    pub async fn head(&self, key: &str) -> ClientResult<Option<VersionedData>> {
        for _ in 0..MAX_REDIRECT_RETRIES {
            let mut client = self.get_client().await;
            let result = client
                .head(HeadRequest {
                    key: key.to_string(),
                })
                .await;

            match result {
                Ok(response) => {
                    return Ok(response.into_inner().value.map(|v| VersionedData {
                        seqno: v.seqno,
                        data: v.data,
                    }));
                }
                Err(status) => match Self::parse_leader_redirect(&status) {
                    Some(leader_addr) => {
                        self.reconnect_to_leader(&leader_addr).await.map_err(|e| {
                            tonic::Status::unavailable(format!(
                                "failed to reconnect to leader at {leader_addr}: {e}"
                            ))
                        })?;
                        continue;
                    }
                    None => return Err(status),
                },
            }
        }
        Err(tonic::Status::unavailable(
            "exceeded maximum leader redirect retries",
        ))
    }

    /// Performs a compare-and-set operation.
    ///
    /// Automatically retries on leader redirect (up to [`MAX_REDIRECT_RETRIES`]
    /// attempts).
    pub async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<u64>,
        new_seqno: u64,
        new_data: Vec<u8>,
    ) -> ClientResult<CaSResult> {
        for _ in 0..MAX_REDIRECT_RETRIES {
            let mut client = self.get_client().await;
            let result = client
                .compare_and_set(CaSRequest {
                    key: key.to_string(),
                    expected,
                    new_seqno,
                    new_data: new_data.clone(),
                })
                .await;

            match result {
                Ok(response) => {
                    let result = response.into_inner().result();
                    return match result {
                        crate::generated::service::ProtoCaSResult::Committed => {
                            Ok(CaSResult::Committed)
                        }
                        crate::generated::service::ProtoCaSResult::ExpectationMismatch => {
                            Ok(CaSResult::ExpectationMismatch)
                        }
                    };
                }
                Err(status) => match Self::parse_leader_redirect(&status) {
                    Some(leader_addr) => {
                        self.reconnect_to_leader(&leader_addr).await.map_err(|e| {
                            tonic::Status::unavailable(format!(
                                "failed to reconnect to leader at {leader_addr}: {e}"
                            ))
                        })?;
                        continue;
                    }
                    None => return Err(status),
                },
            }
        }
        Err(tonic::Status::unavailable(
            "exceeded maximum leader redirect retries",
        ))
    }

    /// Scans versioned data for a key.
    ///
    /// Automatically retries on leader redirect (up to [`MAX_REDIRECT_RETRIES`]
    /// attempts).
    pub async fn scan(&self, key: &str, from: u64, limit: u64) -> ClientResult<Vec<VersionedData>> {
        for _ in 0..MAX_REDIRECT_RETRIES {
            let mut client = self.get_client().await;
            let result = client
                .scan(ScanRequest {
                    key: key.to_string(),
                    from,
                    limit,
                })
                .await;

            match result {
                Ok(response) => {
                    return Ok(response
                        .into_inner()
                        .values
                        .into_iter()
                        .map(|v| VersionedData {
                            seqno: v.seqno,
                            data: v.data,
                        })
                        .collect());
                }
                Err(status) => match Self::parse_leader_redirect(&status) {
                    Some(leader_addr) => {
                        self.reconnect_to_leader(&leader_addr).await.map_err(|e| {
                            tonic::Status::unavailable(format!(
                                "failed to reconnect to leader at {leader_addr}: {e}"
                            ))
                        })?;
                        continue;
                    }
                    None => return Err(status),
                },
            }
        }
        Err(tonic::Status::unavailable(
            "exceeded maximum leader redirect retries",
        ))
    }

    /// Truncates versioned data for a key below a sequence number.
    ///
    /// Automatically retries on leader redirect (up to [`MAX_REDIRECT_RETRIES`]
    /// attempts).
    pub async fn truncate(&self, key: &str, seqno: u64) -> ClientResult<Option<u64>> {
        for _ in 0..MAX_REDIRECT_RETRIES {
            let mut client = self.get_client().await;
            let result = client
                .truncate(TruncateRequest {
                    key: key.to_string(),
                    seqno,
                })
                .await;

            match result {
                Ok(response) => return Ok(response.into_inner().deleted),
                Err(status) => match Self::parse_leader_redirect(&status) {
                    Some(leader_addr) => {
                        self.reconnect_to_leader(&leader_addr).await.map_err(|e| {
                            tonic::Status::unavailable(format!(
                                "failed to reconnect to leader at {leader_addr}: {e}"
                            ))
                        })?;
                        continue;
                    }
                    None => return Err(status),
                },
            }
        }
        Err(tonic::Status::unavailable(
            "exceeded maximum leader redirect retries",
        ))
    }

    /// Lists all keys in the store, returning a streaming response.
    ///
    /// Automatically retries on leader redirect (up to [`MAX_REDIRECT_RETRIES`]
    /// attempts).
    pub async fn list_keys(
        &self,
    ) -> ClientResult<tonic::Streaming<crate::generated::service::ListKeysResponse>> {
        for _ in 0..MAX_REDIRECT_RETRIES {
            let mut client = self.get_client().await;
            let result = client.list_keys(ListKeysRequest {}).await;

            match result {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => match Self::parse_leader_redirect(&status) {
                    Some(leader_addr) => {
                        self.reconnect_to_leader(&leader_addr).await.map_err(|e| {
                            tonic::Status::unavailable(format!(
                                "failed to reconnect to leader at {leader_addr}: {e}"
                            ))
                        })?;
                        continue;
                    }
                    None => return Err(status),
                },
            }
        }
        Err(tonic::Status::unavailable(
            "exceeded maximum leader redirect retries",
        ))
    }
}
