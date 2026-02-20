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

use tonic::transport::Channel;

use crate::generated::service::persist_consensus_service_client::PersistConsensusServiceClient;
use crate::generated::service::{
    CaSRequest, HeadRequest, ListKeysRequest, ScanRequest, TruncateRequest,
};

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

/// A raw gRPC client for the persist-consensus service.
#[derive(Debug, Clone)]
pub struct PersistConsensusClient {
    client: PersistConsensusServiceClient<Channel>,
}

impl PersistConsensusClient {
    /// Connect to a persist-consensus server at the given address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = PersistConsensusServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    /// Returns the latest versioned data for the given key.
    pub async fn head(&self, key: &str) -> ClientResult<Option<VersionedData>> {
        let mut client = self.client.clone();
        let response = client
            .head(HeadRequest {
                key: key.to_string(),
            })
            .await?;

        Ok(response.into_inner().value.map(|v| VersionedData {
            seqno: v.seqno,
            data: v.data,
        }))
    }

    /// Performs a compare-and-set operation.
    pub async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<u64>,
        new_seqno: u64,
        new_data: Vec<u8>,
    ) -> ClientResult<CaSResult> {
        let mut client = self.client.clone();
        let response = client
            .compare_and_set(CaSRequest {
                key: key.to_string(),
                expected,
                new_seqno,
                new_data,
            })
            .await?;

        let result = response.into_inner().result();
        match result {
            crate::generated::service::ProtoCaSResult::Committed => Ok(CaSResult::Committed),
            crate::generated::service::ProtoCaSResult::ExpectationMismatch => {
                Ok(CaSResult::ExpectationMismatch)
            }
        }
    }

    /// Scans versioned data for a key.
    pub async fn scan(
        &self,
        key: &str,
        from: u64,
        limit: u64,
    ) -> ClientResult<Vec<VersionedData>> {
        let mut client = self.client.clone();
        let response = client
            .scan(ScanRequest {
                key: key.to_string(),
                from,
                limit,
            })
            .await?;

        Ok(response
            .into_inner()
            .values
            .into_iter()
            .map(|v| VersionedData {
                seqno: v.seqno,
                data: v.data,
            })
            .collect())
    }

    /// Truncates versioned data for a key below a sequence number.
    pub async fn truncate(&self, key: &str, seqno: u64) -> ClientResult<Option<u64>> {
        let mut client = self.client.clone();
        let response = client
            .truncate(TruncateRequest {
                key: key.to_string(),
                seqno,
            })
            .await?;

        Ok(response.into_inner().deleted)
    }

    /// Lists all keys in the store, returning a streaming response.
    pub async fn list_keys(
        &self,
    ) -> ClientResult<tonic::Streaming<crate::generated::service::ListKeysResponse>> {
        let mut client = self.client.clone();
        let response = client.list_keys(ListKeysRequest {}).await?;
        Ok(response.into_inner())
    }
}
