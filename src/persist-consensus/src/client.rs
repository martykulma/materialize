// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC client implementing the `mz_persist::location::Consensus` trait.

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use mz_persist::location::{
    CaSResult, Consensus, ExternalError, Indeterminate, ResultStream, SeqNo, VersionedData,
};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::generated::service::persist_consensus_service_client::PersistConsensusServiceClient;
use crate::generated::service::{
    CaSRequest, HeadRequest, ListKeysRequest, ProtoCaSResult, ScanRequest, TruncateRequest,
};

/// A `Consensus` implementation that connects to a persist-consensus gRPC service.
#[derive(Debug, Clone)]
pub struct GrpcConsensusClient {
    client: PersistConsensusServiceClient<Channel>,
}

impl GrpcConsensusClient {
    /// Connect to a persist-consensus server at the given address.
    pub async fn connect(addr: String) -> Result<Self, ExternalError> {
        let client = PersistConsensusServiceClient::connect(addr)
            .await
            .map_err(|e| ExternalError::from(anyhow!("gRPC connect error: {e}")))?;
        Ok(Self { client })
    }
}

fn status_to_external_error(status: tonic::Status) -> ExternalError {
    match status.code() {
        tonic::Code::InvalidArgument => {
            ExternalError::from(anyhow!("{}", status.message()))
        }
        _ => ExternalError::Indeterminate(Indeterminate::new(anyhow!(
            "gRPC error: {}",
            status.message()
        ))),
    }
}

#[async_trait]
impl Consensus for GrpcConsensusClient {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let mut client = self.client.clone();
        Box::pin(async_stream::try_stream! {
            let response = client
                .list_keys(ListKeysRequest {})
                .await
                .map_err(status_to_external_error)?;
            let mut stream = response.into_inner();
            while let Some(item) = stream.next().await {
                let item = item.map_err(status_to_external_error)?;
                yield item.key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let mut client = self.client.clone();
        let response = client
            .head(HeadRequest {
                key: key.to_string(),
            })
            .await
            .map_err(status_to_external_error)?;

        Ok(response.into_inner().value.map(|v| VersionedData {
            seqno: SeqNo(v.seqno),
            data: Bytes::from(v.data),
        }))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let mut client = self.client.clone();
        let response = client
            .compare_and_set(CaSRequest {
                key: key.to_string(),
                expected: expected.map(|s| s.0),
                new_seqno: new.seqno.0,
                new_data: new.data.to_vec(),
            })
            .await
            .map_err(status_to_external_error)?;

        let result = response.into_inner().result();
        match result {
            ProtoCaSResult::Committed => Ok(CaSResult::Committed),
            ProtoCaSResult::ExpectationMismatch => Ok(CaSResult::ExpectationMismatch),
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let mut client = self.client.clone();
        let response = client
            .scan(ScanRequest {
                key: key.to_string(),
                from: from.0,
                limit: limit as u64,
            })
            .await
            .map_err(status_to_external_error)?;

        Ok(response
            .into_inner()
            .values
            .into_iter()
            .map(|v| VersionedData {
                seqno: SeqNo(v.seqno),
                data: Bytes::from(v.data),
            })
            .collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let mut client = self.client.clone();
        let response = client
            .truncate(TruncateRequest {
                key: key.to_string(),
                seqno: seqno.0,
            })
            .await
            .map_err(status_to_external_error)?;

        Ok(response.into_inner().deleted.map(|d| d as usize))
    }
}
