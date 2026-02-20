// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by a Raft-based persist-consensus service.

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::url::SensitiveUrl;
use tokio_stream::StreamExt;

use crate::location::{
    CaSResult, Consensus, ExternalError, Indeterminate, ResultStream, SeqNo, VersionedData,
};

/// Configuration to connect to a Raft-backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct RaftConsensusConfig {
    /// The gRPC endpoint of the persist-consensus service (e.g. `raft://host:6880`).
    pub url: SensitiveUrl,
}

impl RaftConsensusConfig {
    /// Returns a new [RaftConsensusConfig] from a URL.
    pub fn new(url: &SensitiveUrl) -> Self {
        RaftConsensusConfig { url: url.clone() }
    }
}

fn status_to_external_error(status: tonic::Status) -> ExternalError {
    match status.code() {
        tonic::Code::InvalidArgument => ExternalError::from(anyhow!("{}", status.message())),
        _ => ExternalError::Indeterminate(Indeterminate::new(anyhow!(
            "gRPC error: {}",
            status.message()
        ))),
    }
}

/// Implementation of [Consensus] that connects to a persist-consensus gRPC service.
pub struct RaftConsensus {
    inner: mz_persist_consensus_client::client::PersistConsensusClient,
}

impl std::fmt::Debug for RaftConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftConsensus").finish_non_exhaustive()
    }
}

impl RaftConsensus {
    /// Open a connection to the Raft consensus service.
    pub async fn open(config: RaftConsensusConfig) -> Result<Self, ExternalError> {
        // Convert raft:// scheme to http:// for gRPC transport.
        let mut addr = config.url.to_string();
        if addr.starts_with("raft://") {
            addr = format!("http://{}", &addr["raft://".len()..]);
        }
        let inner =
            mz_persist_consensus_client::client::PersistConsensusClient::connect(addr)
                .await
                .map_err(|e| ExternalError::from(anyhow!("gRPC connect error: {e}")))?;
        Ok(RaftConsensus { inner })
    }
}

#[async_trait]
impl Consensus for RaftConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let inner = self.inner.clone();
        Box::pin(async_stream::try_stream! {
            let mut stream = inner.list_keys().await.map_err(status_to_external_error)?;
            while let Some(item) = stream.next().await {
                let item = item.map_err(status_to_external_error)?;
                yield item.key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        self.inner
            .head(key)
            .await
            .map(|opt| {
                opt.map(|v| VersionedData {
                    seqno: SeqNo(v.seqno),
                    data: Bytes::from(v.data),
                })
            })
            .map_err(status_to_external_error)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let result = self
            .inner
            .compare_and_set(key, expected.map(|s| s.0), new.seqno.0, new.data.to_vec())
            .await
            .map_err(status_to_external_error)?;

        match result {
            mz_persist_consensus_client::client::CaSResult::Committed => {
                Ok(CaSResult::Committed)
            }
            mz_persist_consensus_client::client::CaSResult::ExpectationMismatch => {
                Ok(CaSResult::ExpectationMismatch)
            }
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.inner
            .scan(key, from.0, limit as u64)
            .await
            .map(|vals| {
                vals.into_iter()
                    .map(|v| VersionedData {
                        seqno: SeqNo(v.seqno),
                        data: Bytes::from(v.data),
                    })
                    .collect()
            })
            .map_err(status_to_external_error)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        self.inner
            .truncate(key, seqno.0)
            .await
            .map(|opt| opt.map(|d| d as usize))
            .map_err(status_to_external_error)
    }
}
