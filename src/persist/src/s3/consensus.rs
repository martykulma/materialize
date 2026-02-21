use std::{
    collections::BTreeMap,
    fmt::{Formatter, Write},
    num::ParseIntError,
    str::FromStr,
};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::{Region, sts::AssumeRoleProvider};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client as S3Client,
    primitives::{ByteStream, SdkBody},
};
use bytes::Buf;
use md5::{self, Digest, Md5};
use mz_ore::url::SensitiveUrl;

use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Configuration to connect to a S3 backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct S3ConsensusConfig {
    bucket: String,
    prefix: String, // this is either empty string or a string with a terminal slash
    client: S3Client,
}

impl S3ConsensusConfig {
    #[allow(dead_code)]
    const EXTERNAL_TESTS_URL: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3CONSENSUS_URL";

    #[allow(dead_code)]
    async fn new_for_test() -> Result<Self, anyhow::Error> {
        let url = std::env::var(Self::EXTERNAL_TESTS_URL)
            .map_err(|_| anyhow!("env variable {} is not set", Self::EXTERNAL_TESTS_URL))?;
        let url = SensitiveUrl::from_str(&url)
            .map_err(|e| e.to_string())
            .map_err(anyhow::Error::msg)?;
        Self::try_from(&url).await
    }

    /// Parse an S3 URL into an S3 consensus configuration
    pub async fn try_from(url: &SensitiveUrl) -> Result<Self, anyhow::Error> {
        assert_eq!("s3", url.0.scheme());
        let bucket = url
            .host()
            .ok_or_else(|| anyhow!("missing bucket: {}", &url.as_str()))?
            .to_string();

        let mut prefix = url
            .path()
            .trim_start_matches('/')
            .trim_end_matches(S3_CONSENSUS_DELIMITER)
            .to_string();
        if !prefix.is_empty() {
            prefix.push(S3_CONSENSUS_DELIMITER);
        }
        let mut query_params = url.query_pairs().collect::<BTreeMap<_, _>>();
        let role_arn = query_params.remove("role_arn").map(|x| x.into_owned());
        let endpoint = query_params.remove("endpoint").map(|x| x.into_owned());
        let region = query_params.remove("region").map(|x| x.into_owned());

        let credentials = match url.password() {
            None => None,
            Some(password) => Some((
                String::from_utf8_lossy(&urlencoding::decode_binary(url.username().as_bytes()))
                    .into_owned(),
                String::from_utf8_lossy(&urlencoding::decode_binary(password.as_bytes()))
                    .into_owned(),
            )),
        };

        let mut loader = mz_aws_util::defaults();
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        };

        if let Some(role_arn) = role_arn {
            let assume_role_sdk_config = mz_aws_util::defaults().load().await;
            let role_provider = AssumeRoleProvider::builder(role_arn)
                .configure(&assume_role_sdk_config)
                .session_name("consensus")
                .build()
                .await;
            loader = loader.credentials_provider(role_provider);
        }

        if let Some((access_key_id, secret_access_key)) = credentials {
            loader = loader.credentials_provider(Credentials::from_keys(
                access_key_id,
                secret_access_key,
                None,
            ));
        }

        if let Some(endpoint) = endpoint {
            loader = loader.endpoint_url(endpoint)
        }

        let client = mz_aws_util::s3::new_client(&loader.load().await);
        Ok(S3ConsensusConfig {
            bucket,
            prefix,
            client,
        })
    }
}

const S3_CONSENSUS_DELIMITER: char = '/';
const S3_CONSENSUS_HEAD: &str = "head_v1";
const S3_CONSENSUS_TAIL: &str = "tail_v1";
/// Hold onto the things we need to make consensus happen in S3.
#[derive(Debug)]
pub struct S3Consensus {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3Consensus {
    /// Build a new [S3Consensus]
    /// It's only async for consensus_impl_test
    pub async fn new(config: S3ConsensusConfig) -> Result<Self, ExternalError> {
        Ok(S3Consensus {
            client: config.client,
            bucket: config.bucket,
            prefix: config.prefix,
        })
    }

    fn base_key(&self, key: &str) -> String {
        let mut res = self.prefix.clone();
        res.push_str(key);
        res.push(S3_CONSENSUS_DELIMITER);
        res
    }

    fn build_s3_key(&self, key: &str, obj_name: &str) -> String {
        let mut res = self.base_key(key);
        res.push_str(obj_name);
        res
    }

    /// Reads the metadata file (head or tail).
    ///
    /// Returns Ok(None) if the file does not exist
    /// Returns Ok(SeqNo) if the file did exist and was parsed
    /// Returns Err(e) otherwise
    async fn read_metadata_file(
        &self,
        key: &str,
        name: &str,
    ) -> Result<Option<S3SeqNo>, anyhow::Error> {
        let res = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, name))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|svc_err| svc_err.is_no_such_key())
                {
                    return Ok(None);
                } else {
                    return Err(anyhow!("read_metadata_file {name}: get_object: {e:?}"));
                }
            }
        };

        res.body
            .collect()
            .await
            .map(|bites| Some(bites.into_bytes().get_u64().into()))
            .map_err(|e| anyhow!("read_metdata_file {name}: body: {e:?}"))
    }

    async fn write_head(
        &self,
        key: &str,
        prev: Option<&SeqNo>,
        next: &SeqNo,
    ) -> Result<(), anyhow::Error> {
        let next_bytes = next.0.to_be_bytes();
        let body = SdkBody::from(&next_bytes[..]);

        tracing::info!("write_head: key:{key} prev:{prev:?} next:{next:?}");

        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(key, S3_CONSENSUS_HEAD))
            .body(ByteStream::new(body));

        let req = if let Some(prev_seqno) = prev {
            let prev_bytes = prev_seqno.0.to_be_bytes();
            let mut h = Md5::new();
            h.update(prev_bytes);
            let etag_bin = h.finalize();
            // etag = 32 (md5) + 2 (enclosing quotes)
            let mut etag = String::with_capacity(34);
            etag.push('"');
            for bite in etag_bin {
                write!(&mut etag, "{:02x}", bite).expect("Unable to write");
            }
            etag.push('"');
            req.if_match(etag)
        } else {
            req.if_none_match("*")
        };
        match req.send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.raw_response()
                    .map(|r| r.status().as_u16())
                    .is_some_and(|s| s == 412 || s == 409)
                {
                    // 412 = PreconditionFailed - mismatch
                    // 409 = ConditionalRequestConflict
                    // ok if this fails, it just means we're stale
                    Ok(())
                } else {
                    Err(anyhow!("write_head: failed with {:?}", e))
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct S3SeqNo(u64);

impl std::fmt::Display for S3SeqNo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

impl FromStr for S3SeqNo {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(S3SeqNo(s.parse::<u64>()?))
    }
}

impl From<&SeqNo> for S3SeqNo {
    fn from(value: &SeqNo) -> Self {
        value.0.into()
    }
}

impl From<SeqNo> for S3SeqNo {
    fn from(value: SeqNo) -> Self {
        value.0.into()
    }
}

impl From<u64> for S3SeqNo {
    fn from(value: u64) -> Self {
        S3SeqNo(value)
    }
}

impl Into<SeqNo> for &S3SeqNo {
    fn into(self) -> SeqNo {
        SeqNo(self.0)
    }
}

impl Into<SeqNo> for S3SeqNo {
    fn into(self) -> SeqNo {
        SeqNo(self.0)
    }
}

/// Implementation of [Consensus] using S3.
/// This implementionation takes advantage of S3's conditional PutObject request to ensure
/// that only a single writer successfully writes an object.
///
/// Consensus relies on storing some [VersionedData] for a given key with a monotonic integer
/// sequence number [SeqNo].
///
/// 1. Once a SeqNo is written, it is never changed.
/// 2. Once a SeqNo is deleted, it is never revived.
///
/// This implementation accepts a bucket and perfix (s3_prefix) as
/// the storage location of the consensus data. Access to the most recent keys, and the ability
/// to find the largest [SeqNo], are latency sensitive.
///
/// The resulting S3 key representation:
/// s3://<bucket>/<s3_prefix>/<key>/<sequence_number>
/// where the sequence number is a 20 character string padded with 0.
///
/// Note: Classic S3 bucket listing returns keys in lexicographic order, but directory buckets
/// do not (S3 Express One Zone). This implementation does not rely on ordering.
///
/// We never delete keys.  Truncate just updates the min SeqNo in [`S3_CONSENSUS_TRUNCATE_FILE`].
/// S3 likes files.
#[async_trait]
impl Consensus for S3Consensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        Box::pin(try_stream! {
            let mut continuation_token = None;
            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&self.prefix)
                    .delimiter(S3_CONSENSUS_DELIMITER);
                if let Some(token) = continuation_token {
                    req = req.continuation_token(token);
                }
                let res = req.send().await.map_err(anyhow::Error::msg)?;
                for entry in res.common_prefixes() {
                    let Some(key) = entry.prefix() else { continue };
                    let key = &key[self.prefix.len()..key.len()-1];
                    yield key.to_string();
                }
                continuation_token = res.continuation_token().map(|s| s.to_string());
                if continuation_token.is_none() {
                    break;
                }
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let Some(s3_seqno) = self.read_metadata_file(key, S3_CONSENSUS_HEAD).await? else {
            return Ok(None);
        };

        let res = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, s3_seqno.to_string().as_str()))
            .send()
            .await
            .map_err(|e| anyhow!("head: get_object: {:?}", e.into_service_error()))?;

        let data = res
            .body
            .collect()
            .await
            .map_err(|e| anyhow!("head: body: {:?}", e))?;

        let versioned_data = VersionedData {
            seqno: s3_seqno.into(),
            data: data.into_bytes(),
        };
        Ok(Some(versioned_data))
    }

    /// This operation always performs 2 S3 write operations:
    /// 1. Write the versioned data object
    /// 2. Write the log head file for the key with the current [`SeqNo`]
    ///
    /// The general algorithm is this:
    /// Conditionally write the [`VersionedData`] file, expecting it not to exist. If the write
    /// succeeds or fails due to conflict, write the log head file.  In the conflict cases, the file
    /// already exists or was concurrently written by another writer. S3 conditional writes are
    /// atomic, so a writer succeeded. The log head file is written in either case to ensure the
    /// system makes progress, as a writer may succeed in writing the [`VersionedData`] and die. The
    /// common case is that both will succeed. In the error case a write may not be visible until
    /// the next `compare_and_set` for the same key.
    ///
    /// Assumptions of the caller:
    /// - Calls [`Consensus::head()`] to get the current [`SeqNo`].
    /// - Calls `compare_and_set` with the expected [`SeqNo`] from head, and a [`VersionedData`]
    ///   with a new [`SeqNo`] of expected + 1.
    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if new.seqno.0 > i64::MAX as u64 {
            return Err(anyhow!(
                "new seqno must be in the range [0, i64::MAX]. Got new: {:?}",
                new.seqno
            )
            .into());
        }

        if let Some(seqno) = expected {
            if new.seqno <= seqno {
                // Don't change this error string, consensus_impl_test matches on this error.
                return Err(anyhow!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, seqno).into());
            } else if new.seqno != seqno.next() {
                // TODO (maz): this is a hack because the client in the test is not well behaved
                tracing::info!("cas: seqno:{seqno:?} new:{:?}", new.seqno);
                return Ok(CaSResult::ExpectationMismatch);
            }
        }

        let body = SdkBody::from(new.data);
        let new_s3_seqno = S3SeqNo::from(&new.seqno);
        let result = match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, new_s3_seqno.to_string().as_str()))
            .body(ByteStream::new(body))
            .if_none_match("*") // condition that states the object should not exist
            .send()
            .await
        {
            Ok(_) => Ok(CaSResult::Committed),
            Err(e) => {
                if e.raw_response()
                    .map(|r| r.status().as_u16())
                    .is_some_and(|s| s == 412 || s == 409)
                {
                    // 412 = PreconditionFailed - mismatch
                    // 409 = ConditionalRequestConflict
                    Ok(CaSResult::ExpectationMismatch)
                } else {
                    Err(anyhow!("compare_and_set: {:?}", e.as_service_error()).into())
                }
            }
        };

        if result.is_ok() {
            if let Err(e) = self.write_head(key, expected.as_ref(), &new.seqno).await {
                // it's ok if this fails, but we'll log it for debugging
                tracing::info!("{e:?}");
            }
        }

        result
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        tracing::info!("scan: {key} {from:?}");
        let Some(head) = self.read_metadata_file(key, S3_CONSENSUS_HEAD).await? else {
            return Ok(vec![]);
        };
        if from.0 > head.0 {
            return Ok(vec![]);
        }

        let tail = self
            .read_metadata_file(key, S3_CONSENSUS_TAIL)
            .await?
            .map(|t| if from.0 > t.0 { from.into() } else { t })
            .unwrap_or_else(|| from.into());

        let total: usize = (head.0 - tail.0)
            .try_into()
            .expect("scan range <= usize::MAX");
        let mut results = Vec::with_capacity(std::cmp::min(limit, total));

        // scan must return results in asc order
        for i in tail.0..=head.0 {
            let s3_seqno = S3SeqNo::from(i);

            let res = match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(self.build_s3_key(key, s3_seqno.to_string().as_str()))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    // per consensus_impl_test the expected behavior is to return any keys found
                    if e.as_service_error()
                        .is_some_and(|svc_err| svc_err.is_no_such_key())
                    {
                        continue;
                    } else {
                        return Err(anyhow!("scan: get_object: {e:?}").into());
                    }
                }
            };

            results.push(VersionedData {
                seqno: s3_seqno.into(),
                data: res
                    .body
                    .collect()
                    .await
                    .map_err(|e| anyhow!("scan: body: {:?}", e))?
                    .into_bytes(),
            });
            if results.len() == limit {
                break;
            }
        }
        Ok(results)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        tracing::info!("truncate: {key} {seqno:?}");
        let Some(head) = self.read_metadata_file(key, S3_CONSENSUS_HEAD).await? else {
            // this must error
            return Err(anyhow!("no entry for key '{key}'").into());
        };

        if seqno.0 > head.0 {
            return Err(anyhow!("seqno must be <= head: head:{head:?} seqno:{seqno:?}",).into());
        }

        // we don't use read_metadata_file because this needs the s3 response from the etag
        let etag_and_seqno = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, S3_CONSENSUS_TAIL))
            .send()
            .await
        {
            Ok(res) => {
                let current_etag = res.e_tag().expect("etag for trunate file").to_string();

                let s3_seqno = res
                    .body
                    .collect()
                    .await
                    .map(|bites| bites.into_bytes().get_u64())
                    .map_err(|e| anyhow!("truncate {S3_CONSENSUS_TAIL}: body: {e:?}"))?;

                Some((current_etag, s3_seqno))
            }
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|svc_err| svc_err.is_no_such_key())
                {
                    None
                } else {
                    return Err(anyhow!(
                        "truncate {S3_CONSENSUS_TAIL}: get_object: {:?}",
                        e.into_service_error()
                    )
                    .into());
                }
            }
        };

        let s3_seqno = if let Some((_, s3_seqno)) = etag_and_seqno {
            if seqno.0 < s3_seqno {
                return Ok(None);
            } else {
                s3_seqno
            }
        } else {
            0
        };

        let next_bytes = seqno.0.to_be_bytes();
        let body = SdkBody::from(&next_bytes[..]);
        let mut put_req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, S3_CONSENSUS_TAIL))
            .body(ByteStream::new(body));

        put_req = if let Some((etag, _)) = etag_and_seqno {
            put_req.if_match(etag)
        } else {
            put_req.if_none_match("*")
        };

        match put_req.send().await {
            Ok(_) => Ok(Some(
                (seqno.0 - s3_seqno)
                    .try_into()
                    .expect("truncate result <= usize::MAX"),
            )),
            // TODO (maz): we may want to log this if the error is not a conflict
            Err(e) => {
                Err(anyhow!("truncate {S3_CONSENSUS_TAIL}: {:?}", e.as_service_error()).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::consensus_impl_test;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn s3_consensus() -> Result<(), ExternalError> {
        let config = S3ConsensusConfig::new_for_test().await?;
        consensus_impl_test(|| S3Consensus::new(config.clone())).await?;
        Ok(())
    }
}
