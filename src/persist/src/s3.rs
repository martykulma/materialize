// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An S3 implementation of [Blob] storage.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::num::ParseIntError;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64};
use std::time::{Duration, Instant};
use std::{cmp, i32, u64};

use anyhow::{Context, anyhow};
use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::sts::AssumeRoleProvider;
use aws_config::timeout::TimeoutConfig;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{AsyncSleep, Sleep};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_types::region::Region;
use bytes::Bytes;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::lgbytes::MetricsRegion;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::RuntimeExt;
use mz_ore::url::SensitiveUrl;
use tokio::runtime::Handle as AsyncHandle;
use tracing::{Instrument, debug, debug_span, trace, trace_span};
use uuid::Uuid;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{
    Blob, BlobMetadata, CaSResult, Consensus, Determinate, ExternalError, ResultStream, SeqNo,
    VersionedData,
};
use crate::metrics::S3BlobMetrics;

/// Configuration for opening an [S3Blob].
#[derive(Clone, Debug)]
pub struct S3BlobConfig {
    metrics: S3BlobMetrics,
    client: S3Client,
    bucket: String,
    prefix: String,
    cfg: Arc<ConfigSet>,
    is_cc_active: bool,
}

// There is no simple way to hook into the S3 client to capture when its various timeouts
// are hit. Instead, we pass along marker values that inform our [MetricsSleep] impl which
// type of timeout was requested so it can substitute in a dynamic value set by config
// from the caller.
const OPERATION_TIMEOUT_MARKER: Duration = Duration::new(111, 1111);
const OPERATION_ATTEMPT_TIMEOUT_MARKER: Duration = Duration::new(222, 2222);
const CONNECT_TIMEOUT_MARKER: Duration = Duration::new(333, 3333);
const READ_TIMEOUT_MARKER: Duration = Duration::new(444, 4444);

#[derive(Debug)]
struct MetricsSleep {
    knobs: Box<dyn BlobKnobs>,
    metrics: S3BlobMetrics,
}

impl AsyncSleep for MetricsSleep {
    fn sleep(&self, duration: Duration) -> Sleep {
        let (duration, metric) = match duration {
            OPERATION_TIMEOUT_MARKER => (
                self.knobs.operation_timeout(),
                Some(self.metrics.operation_timeouts.clone()),
            ),
            OPERATION_ATTEMPT_TIMEOUT_MARKER => (
                self.knobs.operation_attempt_timeout(),
                Some(self.metrics.operation_attempt_timeouts.clone()),
            ),
            CONNECT_TIMEOUT_MARKER => (
                self.knobs.connect_timeout(),
                Some(self.metrics.connect_timeouts.clone()),
            ),
            READ_TIMEOUT_MARKER => (
                self.knobs.read_timeout(),
                Some(self.metrics.read_timeouts.clone()),
            ),
            duration => (duration, None),
        };

        // the sleep future we return here will only be polled to
        // completion if its corresponding http request to S3 times
        // out, meaning we can chain incrementing the appropriate
        // timeout counter to when it finishes
        Sleep::new(tokio::time::sleep(duration).map(|x| {
            if let Some(counter) = metric {
                counter.inc();
            }
            x
        }))
    }
}

impl S3BlobConfig {
    const EXTERNAL_TESTS_S3_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET";

    /// Returns a new [S3BlobConfig] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. S3 credentials and region must be available in the process or
    /// environment.
    pub async fn new(
        bucket: String,
        prefix: String,
        role_arn: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        credentials: Option<(String, String)>,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
        cfg: Arc<ConfigSet>,
    ) -> Result<Self, Error> {
        let is_cc_active = knobs.is_cc_active();
        let mut loader = mz_aws_util::defaults();

        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        };

        if let Some(role_arn) = role_arn {
            let assume_role_sdk_config = mz_aws_util::defaults().load().await;
            let role_provider = AssumeRoleProvider::builder(role_arn)
                .configure(&assume_role_sdk_config)
                .session_name("persist")
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

        // NB: we must always use the custom sleep impl if we use the timeout marker values
        loader = loader.sleep_impl(MetricsSleep {
            knobs,
            metrics: metrics.clone(),
        });
        loader = loader.timeout_config(
            TimeoutConfig::builder()
                // maximum time allowed for a top-level S3 API call (including internal retries)
                .operation_timeout(OPERATION_TIMEOUT_MARKER)
                // maximum time allowed for a single network call
                .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT_MARKER)
                // maximum time until a connection succeeds
                .connect_timeout(CONNECT_TIMEOUT_MARKER)
                // maximum time to read the first byte of a response
                .read_timeout(READ_TIMEOUT_MARKER)
                .build(),
        );

        let client = mz_aws_util::s3::new_client(&loader.load().await);
        Ok(S3BlobConfig {
            metrics,
            client,
            bucket,
            prefix,
            cfg,
            is_cc_active,
        })
    }

    /// Returns a new [S3BlobConfig] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like s3) are
    /// no-ops, so that `cargo test` does the right thing without any
    /// configuration. To activate the tests, set the
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET` environment variable and
    /// ensure you have valid AWS credentials available in a location where the
    /// AWS Rust SDK can discovery them.
    ///
    /// This intentionally uses the `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET`
    /// env as the switch for test no-op-ness instead of the presence of a valid
    /// AWS authentication configuration envs because a developers might have
    /// valid credentials present and this isn't an explicit enough signal from
    /// a developer running `cargo test` that it's okay to use these
    /// credentials. It also intentionally does not use the local drop-in s3
    /// replacement to keep persist unit tests light.
    ///
    /// On CI, these tests are enabled by adding the scratch-aws-access plugin
    /// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET` in
    /// `ci/test/cargo-test/mzcompose.py`.
    ///
    /// For a Materialize developer, to opt in to these tests locally for
    /// development, follow the AWS access guide:
    ///
    /// ```text
    /// https://github.com/MaterializeInc/i2/blob/main/doc/aws-access.md
    /// ```
    ///
    /// then running `source src/persist/s3_test_env_mz.sh`. You will also have
    /// to run `aws sso login` if you haven't recently.
    ///
    /// Non-Materialize developers will have to set up their own auto-deleting
    /// bucket and export the same env vars that s3_test_env_mz.sh does.
    ///
    /// Only public for use in src/benches.
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let bucket = match std::env::var(Self::EXTERNAL_TESTS_S3_BUCKET) {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        struct TestBlobKnobs;
        impl std::fmt::Debug for TestBlobKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestBlobKnobs").finish_non_exhaustive()
            }
        }
        impl BlobKnobs for TestBlobKnobs {
            fn operation_timeout(&self) -> Duration {
                OPERATION_TIMEOUT_MARKER
            }

            fn operation_attempt_timeout(&self) -> Duration {
                OPERATION_ATTEMPT_TIMEOUT_MARKER
            }

            fn connect_timeout(&self) -> Duration {
                CONNECT_TIMEOUT_MARKER
            }

            fn read_timeout(&self) -> Duration {
                READ_TIMEOUT_MARKER
            }

            fn is_cc_active(&self) -> bool {
                false
            }
        }

        // Give each test a unique prefix so they don't conflict. We don't have
        // to worry about deleting any data that we create because the bucket is
        // set to auto-delete after 1 day.
        let prefix = Uuid::new_v4().to_string();
        let role_arn = None;
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());
        let config = S3BlobConfig::new(
            bucket,
            prefix,
            role_arn,
            None,
            None,
            None,
            Box::new(TestBlobKnobs),
            metrics,
            Arc::new(
                ConfigSet::default()
                    .add(&ENABLE_S3_LGALLOC_CC_SIZES)
                    .add(&ENABLE_S3_LGALLOC_NONCC_SIZES),
            ),
        )
        .await?;
        Ok(Some(config))
    }

    /// Returns a clone of Self with a new v4 uuid prefix.
    pub fn clone_with_new_uuid_prefix(&self) -> Self {
        let mut ret = self.clone();
        ret.prefix = Uuid::new_v4().to_string();
        ret
    }
}

/// Implementation of [Blob] backed by S3.
#[derive(Debug)]
pub struct S3Blob {
    metrics: S3BlobMetrics,
    client: S3Client,
    bucket: String,
    prefix: String,
    // Maximum number of keys we get information about per list-objects request.
    //
    // Defaults to 1000 which is the current AWS max.
    max_keys: i32,
    multipart_config: MultipartConfig,
    cfg: Arc<ConfigSet>,
    is_cc_active: bool,
}

impl S3Blob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: S3BlobConfig) -> Result<Self, ExternalError> {
        let ret = S3Blob {
            metrics: config.metrics,
            client: config.client,
            bucket: config.bucket,
            prefix: config.prefix,
            max_keys: 1_000,
            multipart_config: MultipartConfig::default(),
            cfg: config.cfg,
            is_cc_active: config.is_cc_active,
        };
        // Connect before returning success. We don't particularly care about
        // what's stored in this blob (nothing writes to it, so presumably it's
        // empty) just that we were able and allowed to fetch it.
        let _ = ret.get("HEALTH_CHECK").await?;
        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

pub(crate) const ENABLE_S3_LGALLOC_CC_SIZES: Config<bool> = Config::new(
    "persist_enable_s3_lgalloc_cc_sizes",
    true,
    "An incident flag to disable copying fetched s3 data into lgalloc on cc sized clusters.",
);

pub(crate) const ENABLE_S3_LGALLOC_NONCC_SIZES: Config<bool> = Config::new(
    "persist_enable_s3_lgalloc_noncc_sizes",
    false,
    "A feature flag to enable copying fetched s3 data into lgalloc on non-cc sized clusters.",
);

#[async_trait]
impl Blob for S3Blob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        // S3 advises that it's fastest to download large objects along the part
        // boundaries they were originally uploaded with [1].
        //
        // [1]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
        //
        // One option is to run the same logic as multipart does and do the
        // requests using the resulting byte ranges, but if we ever changed the
        // multipart chunking logic, they wouldn't line up for old blobs written
        // by a previous version.
        //
        // Another option is to store the part boundaries in the metadata we
        // keep about the batch, but this would be large and wasteful.
        //
        // Luckily, s3 exposes a part_number param on GetObject requests that we
        // can use. If an object was created with multipart, it allows
        // requesting each part as they were originally uploaded by the part
        // number index. With this, we can simply send off requests for part
        // number 1..=num_parts and reassemble the results.
        //
        // We could roundtrip the number of parts through persist batch
        // metadata, but with some cleverness, we can avoid even this. Turns
        // out, if multipart upload wasn't used (it was just a normal PutObject
        // request), s3 will still happily return it for a request specifying a
        // part_number of 1. This lets us fire off a first request, which
        // contains the metadata we need to determine how many additional parts
        // we need, if any.
        //
        // So, the following call sends this first request. The SDK even returns
        // the headers before the full data body has completed. This gives us
        // the number of parts. We can then proceed to fetch the body of the
        // first request concurrently with the rest of the parts of the object.

        // For each header and body that we fetch, we track the fastest, and
        // any large deviations from it.
        let min_body_elapsed = Arc::new(MinElapsed::default());
        let min_header_elapsed = Arc::new(MinElapsed::default());
        self.metrics.get_part.inc();

        // Fetch our first header, this tells us how many more are left.
        let header_start = Instant::now();
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&path)
            .part_number(1)
            .send()
            .await;
        let elapsed = header_start.elapsed();
        min_header_elapsed.observe(elapsed, "s3 download first part header");

        let first_part = match object {
            Ok(object) => object,
            Err(SdkError::ServiceError(err)) if err.err().is_no_such_key() => return Ok(None),
            Err(err) => {
                self.update_error_metrics("GetObject", &err);
                Err(anyhow!(err).context("s3 get meta err"))?
            }
        };

        // Get the remaining number of parts
        let num_parts = match first_part.parts_count() {
            // For a non-multipart upload, parts_count will be None. The rest of  the code works
            // perfectly well if we just pretend this was a multipart upload of 1 part.
            None => 1,
            // For any positive value greater than 0, just return it.
            Some(parts @ 1..) => parts,
            // A non-positive value is invalid.
            Some(bad) => {
                assert!(bad <= 0);
                return Err(anyhow!("unexpected number of s3 object parts: {}", bad).into());
            }
        };

        trace!(
            "s3 download first header took {:?} ({num_parts} parts)",
            start_overall.elapsed(),
        );

        let mut body_futures = FuturesOrdered::new();
        let mut first_part = Some(first_part);

        // Fetch the headers of the rest of the parts. (Starting at part 2 because we already
        // did part 1.)
        for part_num in 1..=num_parts {
            // Clone a handle to our MinElapsed trackers so we can give one to
            // each download task.
            let min_header_elapsed = Arc::clone(&min_header_elapsed);
            let min_body_elapsed = Arc::clone(&min_body_elapsed);
            let get_invalid_resp = self.metrics.get_invalid_resp.clone();
            let first_part = first_part.take();
            let path = &path;
            let request_future = async move {
                // Fetch the headers of the rest of the parts. (Using the existing headers
                // for part 1.
                let mut object = match first_part {
                    Some(first_part) => {
                        assert_eq!(part_num, 1, "only the first part should be prefetched");
                        first_part
                    }
                    None => {
                        assert_ne!(part_num, 1, "first part should be prefetched");
                        // Request our headers.
                        let header_start = Instant::now();
                        let object = self
                            .client
                            .get_object()
                            .bucket(&self.bucket)
                            .key(path)
                            .part_number(part_num)
                            .send()
                            .await
                            .inspect_err(|err| self.update_error_metrics("GetObject", err))
                            .context("s3 get meta err")?;
                        min_header_elapsed
                            .observe(header_start.elapsed(), "s3 download part header");
                        object
                    }
                };

                // Request the body.
                let body_start = Instant::now();
                let mut body_parts: Vec<Bytes> = Vec::new();

                // Get the data into lgalloc at the absolute earliest possible
                // point without (yet) having to fork the s3 client library.
                let enable_s3_lgalloc = if self.is_cc_active {
                    ENABLE_S3_LGALLOC_CC_SIZES.get(&self.cfg)
                } else {
                    ENABLE_S3_LGALLOC_NONCC_SIZES.get(&self.cfg)
                };

                // Copy all of the bytes off the network and into a single allocation.
                let mut buffer = match object.content_length() {
                    Some(len @ 1..) => {
                        let len: u64 = len.try_into().expect("positive integer");
                        // N.B. `lgalloc` cannot reallocate so we need to make sure the initial
                        // allocation is large enough to fit then entire blob.
                        let buf: MetricsRegion<u8> = self
                            .metrics
                            .lgbytes
                            .persist_s3
                            .new_region(usize::cast_from(len));
                        Some(buf)
                    }
                    // content-length of 0 isn't necessarily invalid.
                    Some(len @ ..=-1) => {
                        tracing::trace!(?len, "found invalid content-length, falling back");
                        get_invalid_resp.inc();
                        None
                    }
                    Some(0) | None => None,
                };

                while let Some(data) = object.body.next().await {
                    let data = data.context("s3 get body err")?;
                    match &mut buffer {
                        // Write to our single allocation, if it's enabled.
                        Some(buf) => buf.extend_from_slice(&data[..]),
                        // Fallback to spilling into lgalloc is quick as possible.
                        None if enable_s3_lgalloc => {
                            body_parts.push(self.metrics.lgbytes.persist_s3.try_mmap_bytes(data));
                        }
                        // If all else false just heap allocate.
                        None => {
                            // In the CYA fallback case, make sure we skip the
                            // memcpy to preserve the previous behavior as closely
                            // as possible.
                            //
                            // TODO: Once we've validated the LgBytes path, change
                            // this fallback path to be a heap allocated LgBytes.
                            // Then we can remove the pub from MaybeLgBytes.
                            body_parts.push(data);
                        }
                    }
                }

                // Append our single segment, if it exists.
                if let Some(body) = buffer {
                    // If we're writing into a single buffer we shouldn't have
                    // pushed anything else into our segments.
                    assert!(body_parts.is_empty());
                    body_parts.push(body.into());
                }

                let body_elapsed = body_start.elapsed();
                min_body_elapsed.observe(body_elapsed, "s3 download part body");

                Ok::<_, anyhow::Error>(body_parts)
            };

            body_futures.push_back(request_future);
        }

        // Await on all of our parts requests.
        let mut segments = vec![];
        while let Some(result) = body_futures.next().await {
            // Download failure, we failed to fetch the body from S3.
            let mut part_body = result
                .inspect_err(|e| {
                    self.metrics
                        .error_counts
                        .with_label_values(&["GetObjectStream", e.to_string().as_str()])
                        .inc()
                })
                .context("s3 get body err")?;

            // Collect all of our segments.
            segments.append(&mut part_body);
        }

        debug!(
            "s3 GetObject took {:?} ({} parts)",
            start_overall.elapsed(),
            num_parts
        );
        Ok(Some(SegmentedBytes::from(segments)))
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let mut continuation_token = None;
        // we only want to return keys that match the specified blob key prefix
        let blob_key_prefix = self.get_path(key_prefix);
        // but we want to exclude the shared root prefix from our returned keys,
        // so only the blob key itself is passed in to `f`
        let strippable_root_prefix = format!("{}/", self.prefix);

        loop {
            self.metrics.list_objects.inc();
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&blob_key_prefix)
                .max_keys(self.max_keys)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .inspect_err(|err| self.update_error_metrics("ListObjectsV2", err))
                .context("list bucket error")?;
            if let Some(contents) = resp.contents {
                for object in contents.iter() {
                    if let Some(key) = object.key.as_ref() {
                        if let Some(key) = key.strip_prefix(&strippable_root_prefix) {
                            let size_in_bytes = match object.size {
                                None => {
                                    return Err(ExternalError::from(anyhow!(
                                        "object missing size: {key}"
                                    )));
                                }
                                Some(size) => size
                                    .try_into()
                                    .expect("file in S3 cannot have negative size"),
                            };
                            f(BlobMetadata { key, size_in_bytes });
                        } else {
                            return Err(ExternalError::from(anyhow!(
                                "found key with invalid prefix: {}",
                                key
                            )));
                        }
                    }
                }
            }

            if resp.next_continuation_token.is_some() {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let value_len = value.len();
        if self
            .multipart_config
            .should_multipart(value_len)
            .map_err(anyhow::Error::msg)?
        {
            self.set_multi_part(key, value)
                .instrument(debug_span!("s3set_multi", payload_len = value_len))
                .await
        } else {
            self.set_single_part(key, value).await
        }
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        // There is a race condition here where, if two delete calls for the
        // same key occur simultaneously, both might think they did the actual
        // deletion. This return value is only used for metrics, so it's
        // unfortunate, but fine.
        let path = self.get_path(key);
        self.metrics.delete_head.inc();
        let head_res = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await;
        let size_bytes = match head_res {
            Ok(x) => match x.content_length {
                None => {
                    return Err(ExternalError::from(anyhow!(
                        "s3 delete content length was none"
                    )));
                }
                Some(content_length) => {
                    u64::try_from(content_length).expect("file in S3 cannot have negative size")
                }
            },
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => return Ok(None),
            Err(err) => {
                self.update_error_metrics("HeadObject", &err);
                return Err(ExternalError::from(
                    anyhow!(err).context("s3 delete head err"),
                ));
            }
        };
        self.metrics.delete_object.inc();
        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await
            .inspect_err(|err| self.update_error_metrics("DeleteObject", err))
            .context("s3 delete object err")?;
        Ok(Some(usize::cast_from(size_bytes)))
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        // Fetch the latest version of the object. If it's a normal version, return true;
        // if it's a delete marker, delete it and loop; if there is no such version,
        // return false.
        // TODO: limit the number of delete markers we'll peel back?
        loop {
            // S3 only lets us fetch the versions of an object with a list requests.
            // Seems a bit wasteful to just fetch one at a time, but otherwise we can only
            // guess the order of versions via the timestamp, and that feels brittle.
            let list_res = self
                .client
                .list_object_versions()
                .bucket(&self.bucket)
                .prefix(&path)
                .max_keys(1)
                .send()
                .await
                .inspect_err(|err| self.update_error_metrics("ListObjectVersions", err))
                .context("listing object versions during restore")?;

            let current_delete = list_res
                .delete_markers()
                .into_iter()
                .filter(|d| {
                    // We need to check that any versions we're looking at have the right key,
                    // not just a key with our key as a prefix.
                    d.key() == Some(path.as_str())
                })
                .find(|d| d.is_latest().unwrap_or(false))
                .and_then(|d| d.version_id());

            if let Some(version) = current_delete {
                let deleted = self
                    .client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&path)
                    .version_id(version)
                    .send()
                    .await
                    .inspect_err(|err| self.update_error_metrics("DeleteObject", err))
                    .context("deleting a delete marker")?;
                assert!(
                    deleted.delete_marker().unwrap_or(false),
                    "deleting a delete marker"
                );
            } else {
                let has_current_version = list_res
                    .versions()
                    .into_iter()
                    .filter(|d| d.key() == Some(path.as_str()))
                    .any(|v| v.is_latest().unwrap_or(false));

                if !has_current_version {
                    return Err(Determinate::new(anyhow!(
                        "unable to restore {key} in s3: no valid version exists"
                    ))
                    .into());
                }
                return Ok(());
            }
        }
    }
}

impl S3Blob {
    async fn set_single_part(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        let value_len = value.len();
        let part_span = trace_span!("s3set_single", payload_len = value_len);
        self.metrics.set_single.inc();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(value))
            .send()
            .instrument(part_span)
            .await
            .inspect_err(|err| self.update_error_metrics("PutObject", err))
            .context("set single part")?;
        debug!(
            "s3 PutObject single done {}b / {:?}",
            value_len,
            start_overall.elapsed()
        );
        Ok(())
    }

    // TODO(benesch): remove this once this function no longer makes use of
    // potentially dangerous `as` conversions.
    #[allow(clippy::as_conversions)]
    async fn set_multi_part(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        // Start the multi part request and get an upload id.
        trace!("s3 PutObject multi start {}b", value.len());
        self.metrics.set_multi_create.inc();
        let upload_res = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&path)
            .customize()
            .mutate_request(|req| {
                // By default the Rust AWS SDK does not set the Content-Length
                // header on POST calls with empty bodies. This is fine for S3,
                // but when running against GCS's S3 interop mode these calls
                // will be rejected unless we set this header manually.
                req.headers_mut().insert("Content-Length", "0");
            })
            .send()
            .instrument(debug_span!("s3set_multi_start"))
            .await
            .inspect_err(|err| self.update_error_metrics("CreateMultipartUpload", err))
            .context("create_multipart_upload err")?;
        let upload_id = upload_res
            .upload_id()
            .ok_or_else(|| anyhow!("create_multipart_upload response missing upload_id"))?;
        trace!(
            "s3 create_multipart_upload took {:?}",
            start_overall.elapsed()
        );

        let async_runtime = AsyncHandle::try_current().map_err(anyhow::Error::new)?;

        // Fire off all the individual parts.
        //
        // TODO: The aws cli throttles how many of these are outstanding at any
        // given point. We'll likely want to do the same at some point.
        let start_parts = Instant::now();
        let mut part_futs = Vec::new();
        for (part_num, part_range) in self.multipart_config.part_iter(value.len()) {
            // NB: Without this spawn, these will execute serially. This is rust
            // async 101 stuff, but there isn't much async in the persist
            // codebase (yet?) so I thought it worth calling out.
            let part_span = debug_span!("s3set_multi_part", payload_len = part_range.len());
            let part_fut = async_runtime.spawn_named(
                // TODO: Add the key and part number once this can be annotated
                // with metadata.
                || "persist_s3blob_put_part",
                {
                    self.metrics.set_multi_part.inc();
                    self.client
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(&path)
                        .upload_id(upload_id)
                        .part_number(part_num as i32)
                        .body(ByteStream::from(value.slice(part_range)))
                        .send()
                        .instrument(part_span)
                        .map(move |res| (start_parts.elapsed(), res))
                },
            );
            part_futs.push((part_num, part_fut));
        }
        let parts_len = part_futs.len();

        // Wait on all the parts to finish. This is done in part order, no need
        // for joining them in the order they finish.
        //
        // TODO: Consider using something like futures::future::join_all() for
        // this. That would cancel outstanding requests for us if any of them
        // fails. However, it might not play well with using retries for tail
        // latencies. Investigate.
        let min_part_elapsed = MinElapsed::default();
        let mut parts = Vec::with_capacity(parts_len);
        for (part_num, part_fut) in part_futs.into_iter() {
            let (this_part_elapsed, part_res) = part_fut
                .inspect(|_| {
                    self.metrics
                        .error_counts
                        .with_label_values(&["UploadPart", "AsyncSpawnError"])
                        .inc()
                })
                .await;
            let part_res = part_res
                .inspect_err(|err| self.update_error_metrics("UploadPart", err))
                .context("s3 upload_part err")?;
            let part_e_tag = part_res.e_tag().ok_or_else(|| {
                self.metrics
                    .error_counts
                    .with_label_values(&["UploadPart", "MissingEtag"])
                    .inc();
                anyhow!("s3 upload part missing e_tag")
            })?;
            parts.push(
                CompletedPart::builder()
                    .e_tag(part_e_tag)
                    .part_number(part_num as i32)
                    .build(),
            );
            min_part_elapsed.observe(this_part_elapsed, "s3 upload_part took");
        }
        trace!(
            "s3 upload_parts overall took {:?} ({} parts)",
            start_parts.elapsed(),
            parts_len
        );

        // Complete the upload.
        //
        // Currently, we early return if any of the individual parts fail. This
        // permanently orphans any parts that succeeded. One fix is to call
        // abort_multipart_upload, which deletes them. However, there's also an
        // option for an s3 bucket to auto-delete parts that haven't been
        // completed or aborted after a given amount of time. This latter is
        // simpler and also resilient to ill-timed mz restarts, so we use it for
        // now. We could likely add the accounting necessary to make
        // abort_multipart_upload work, but it would be complex and affect perf.
        // Let's see how far we can get without it.
        let start_complete = Instant::now();
        self.metrics.set_multi_complete.inc();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&path)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .instrument(debug_span!("s3set_multi_complete", num_parts = parts_len))
            .await
            .inspect_err(|err| self.update_error_metrics("CompleteMultipartUpload", err))
            .context("complete_multipart_upload err")?;
        trace!(
            "s3 complete_multipart_upload took {:?}",
            start_complete.elapsed()
        );

        debug!(
            "s3 PutObject multi done {}b / {:?} ({} parts)",
            value.len(),
            start_overall.elapsed(),
            parts_len
        );
        Ok(())
    }

    fn update_error_metrics<E, R>(&self, op: &str, err: &SdkError<E, R>)
    where
        E: ProvideErrorMetadata,
    {
        let code = match err {
            SdkError::ServiceError(e) => match e.err().code() {
                Some(code) => code,
                None => "UnknownServiceError",
            },
            SdkError::DispatchFailure(e) => {
                if let Some(other_error) = e.as_other() {
                    match other_error {
                        aws_config::retry::ErrorKind::TransientError => "TransientError",
                        aws_config::retry::ErrorKind::ThrottlingError => "ThrottlingError",
                        aws_config::retry::ErrorKind::ServerError => "ServerError",
                        aws_config::retry::ErrorKind::ClientError => "ClientError",
                        _ => "UnknownDispatchFailure",
                    }
                } else if e.is_timeout() {
                    "TimeoutError"
                } else if e.is_io() {
                    "IOError"
                } else if e.is_user() {
                    "UserError"
                } else {
                    "UnknownDispathFailure"
                }
            }
            SdkError::ResponseError(_) => "ResponseError",
            SdkError::ConstructionFailure(_) => "ConstructionFailure",
            // There is some overlap with MetricsSleep. MetricsSleep is more granular
            // but does not contain the operation.
            SdkError::TimeoutError(_) => "TimeoutError",
            // an error was added at some point in the future
            _ => "UnknownSdkError",
        };
        self.metrics
            .error_counts
            .with_label_values(&[op, code])
            .inc();
    }
}

#[derive(Clone, Debug)]
struct MultipartConfig {
    multipart_threshold: usize,
    multipart_chunk_size: usize,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: Self::DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: Self::DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }
}

const MB: usize = 1024 * 1024;
const TB: usize = 1024 * 1024 * MB;

impl MultipartConfig {
    /// The minimum object size for which we start using multipart upload.
    ///
    /// From the official `aws cli` tool implementation:
    ///
    /// <https://github.com/aws/aws-cli/blob/2.4.14/awscli/customizations/s3/transferconfig.py#L18-L29>
    const DEFAULT_MULTIPART_THRESHOLD: usize = 8 * MB;
    /// The size of each part (except the last) in a multipart upload.
    ///
    /// From the official `aws cli` tool implementation:
    ///
    /// <https://github.com/aws/aws-cli/blob/2.4.14/awscli/customizations/s3/transferconfig.py#L18-L29>
    const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 8 * MB;

    /// The largest size object creatable in S3.
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MAX_SINGLE_UPLOAD_SIZE: usize = 5 * TB;
    /// The minimum size of a part in a multipart upload.
    ///
    /// This minimum doesn't apply to the last chunk, which can be any size.
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MIN_UPLOAD_CHUNK_SIZE: usize = 5 * MB;
    /// The smallest allowable part number (inclusive).
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MIN_PART_NUM: u32 = 1;
    /// The largest allowable part number (inclusive).
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MAX_PART_NUM: u32 = 10_000;

    fn should_multipart(&self, blob_len: usize) -> Result<bool, String> {
        if blob_len > Self::MAX_SINGLE_UPLOAD_SIZE {
            return Err(format!(
                "S3 does not support blobs larger than {} bytes got: {}",
                Self::MAX_SINGLE_UPLOAD_SIZE,
                blob_len
            ));
        }
        Ok(blob_len > self.multipart_threshold)
    }

    fn part_iter(&self, blob_len: usize) -> MultipartChunkIter {
        debug_assert!(self.multipart_chunk_size >= MultipartConfig::MIN_UPLOAD_CHUNK_SIZE);
        MultipartChunkIter::new(self.multipart_chunk_size, blob_len)
    }
}

#[derive(Clone, Debug)]
struct MultipartChunkIter {
    total_len: usize,
    part_size: usize,
    part_idx: u32,
}

impl MultipartChunkIter {
    fn new(default_part_size: usize, blob_len: usize) -> Self {
        let max_parts: usize = usize::cast_from(MultipartConfig::MAX_PART_NUM);

        // Compute the minimum part size we can use without going over the max
        // number of parts that S3 allows: `ceil(blob_len / max_parts)`.This
        // will end up getting thrown away by the `cmp::max` for anything
        // smaller than `max_parts * default_part_size = 80GiB`.
        let min_part_size = (blob_len + max_parts - 1) / max_parts;
        let part_size = cmp::max(min_part_size, default_part_size);

        // Part nums are 1-indexed in S3. Convert back to 0-indexed to make the
        // range math easier to follow.
        let part_idx = MultipartConfig::MIN_PART_NUM - 1;
        MultipartChunkIter {
            total_len: blob_len,
            part_size,
            part_idx,
        }
    }
}

impl Iterator for MultipartChunkIter {
    type Item = (u32, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let part_idx = self.part_idx;
        self.part_idx += 1;

        let start = usize::cast_from(part_idx) * self.part_size;
        if start >= self.total_len {
            return None;
        }
        let end = cmp::min(start + self.part_size, self.total_len);
        let part_num = part_idx + 1;
        Some((part_num, start..end))
    }
}

/// A helper for tracking the minimum of a set of Durations.
#[derive(Debug)]
struct MinElapsed {
    min: AtomicU64,
    alert_factor: u64,
}

impl Default for MinElapsed {
    fn default() -> Self {
        MinElapsed {
            min: AtomicU64::new(u64::MAX),
            alert_factor: 8,
        }
    }
}

impl MinElapsed {
    fn observe(&self, x: Duration, msg: &'static str) {
        let nanos = x.as_nanos();
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);

        // Possibly set a new minimum.
        let prev_min = self.min.fetch_min(nanos, atomic::Ordering::SeqCst);

        // Trace if our provided duration was much larger than our minimum.
        let new_min = std::cmp::min(prev_min, nanos);
        if nanos > new_min.saturating_mul(self.alert_factor) {
            let min_duration = Duration::from_nanos(new_min);
            let factor = self.alert_factor;
            debug!("{msg} took {x:?} more than {factor}x the min {min_duration:?}");
        } else {
            trace!("{msg} took {x:?}");
        }
    }
}

// Make sure the "vendored" feature of the openssl_sys crate makes it into the
// transitive dep graph of persist, so that we don't attempt to link against the
// system OpenSSL library. Fake a usage of the crate here so that a good
// samaritan doesn't remove our unused dep.
#[allow(dead_code)]
fn openssl_sys_hack() {
    openssl_sys::init();
}
/// Configuration to connect to a S3 backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct S3ConsensusConfig {
    bucket: String,
    prefix: String,
    client: S3Client,
}

impl S3ConsensusConfig {
    #[allow(dead_code)]
    const EXTERNAL_TESTS_URL: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3CONSENSUS_URL";

    #[allow(dead_code)]
    async fn new_for_test() -> Result<Self, anyhow::Error> {
        let url = std::env::var(Self::EXTERNAL_TESTS_URL)?;
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
        let prefix = url
            .path()
            .strip_prefix('/')
            .unwrap_or_else(|| url.path())
            .to_string();
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

/// boo
#[derive(Debug)]
pub struct S3Consensus {
    client: S3Client,
    bucket: String,
    prefix: String,
}
/// boo
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

    fn build_s3_key(&self, key: &str, seqno: &S3SeqNo) -> String {
        // we want most recent version to be lexicographically first
        format!("{}/{}/{}", self.prefix, key, seqno)
    }

    fn parse_s3_key(&self, s3_key: &str) -> (String, S3SeqNo) {
        // we know that the version at the end will always be 20 chars long
        let key = &s3_key[(self.prefix.len() + 1)..(s3_key.len() - 21)];
        let ver = &s3_key[s3_key.len() - 20..];
        (
            key.to_string(),
            ver.parse::<S3SeqNo>().expect("valid version"),
        )
    }

    async fn s3_list<F>(&self, prefix: String, mut process_page: F) -> Result<(), anyhow::Error>
    where
        F: FnMut(ListObjectsV2Output) -> Result<bool, anyhow::Error>,
    {
        let mut continuation_token = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .delimiter(S3_CONSENSUS_DELIMITER);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }
            let res = req.send().await.map_err(anyhow::Error::msg)?;

            continuation_token = res.continuation_token().map(|s| s.to_string());
            if !process_page(res)? || continuation_token.is_none() {
                break;
            }
        }
        Ok(())
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
        // No special handling for overflow as both S3SeqNo and SeqNo are u64 internally
        S3SeqNo(u64::MAX - value.0)
    }
}

impl Into<SeqNo> for &S3SeqNo {
    fn into(self) -> SeqNo {
        // No special handling for overflow as both S3SeqNo and SeqNo are u64 internally
        SeqNo(u64::MAX - self.0)
    }
}

const S3_CONSENSUS_DELIMITER: &str = "/";

// We can't use Etag (String, generally MD5, but not always) or VersionId (random UUID)
// s3://<bucket>/<prefix>/<key>/<version>
// where version is SeqNo and we idenify collision using PutObject If-None-Match: "*"
// this relies on lexicographic ordering, so directory buckets are a no-no

/// Implementation of [Consensus] using S3.
/// This implementionation takes advantage of S3's conditional PutObject request to ensure
/// that only a single writer successfully writes an object.
///
/// Consensus relies on storing some [VersionedData] for a given key with a monotonic integer
/// sequence number [SeqNo].  This implementation accepts a bucket and perfix (s3_prefix) as
/// the storage location of the consensus data. Access to the most recent keys, and the ability
/// to find the largest [SeqNo], are latency sensitive.  To address that need, the S3 key is
/// written in such a way that sequence numbers are descending.
///
/// The resulting S3 key representation:
/// s3://<bucket>/<s3_prefix>/<key>/<u64::MAX - seqno>
/// where the sequence number is a 20 character string padded with 0.
///
/// Note: Classic S3 bucket listing returns keys in lexicographic order, but directory buckets
/// do not.  This implementation relies on lexicographically ordered listing, so directory buckets
/// cannot be supported with modification.
#[async_trait]
impl Consensus for S3Consensus {
    fn list_keys(&self) -> ResultStream<String> {
        Box::pin(try_stream! {
            let mut continuation_token = None;
            let prefix = format!("{}/", self.prefix);
            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&prefix)
                    .delimiter(S3_CONSENSUS_DELIMITER);
                if let Some(token) = continuation_token {
                    req = req.continuation_token(token);
                }
                let res = req.send().await.map_err(anyhow::Error::msg)?;
                for entry in res.common_prefixes() {
                    let Some(key) = entry.prefix() else { continue };
                    let key = &key[prefix.len()..key.len()-1];
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
        let res = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{}/{}/", self.prefix, key))
            .delimiter(S3_CONSENSUS_DELIMITER)
            .max_keys(1)
            .send()
            .await
            .map_err(anyhow::Error::msg)?;

        let Some((_, s3_seqno)) = res
            .contents()
            .iter()
            .next()
            .map(|k| k.key().map(|s3_key| self.parse_s3_key(s3_key)))
            .flatten()
        else {
            return Ok(None);
        };

        let res = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, &s3_seqno))
            .send()
            .await
            .map_err(anyhow::Error::msg)?;

        let data = res.body.collect().await.map_err(anyhow::Error::msg)?;
        let versioned_data = VersionedData {
            seqno: (&s3_seqno).into(),
            data: data.into_bytes(),
        };
        Ok(Some(versioned_data))
    }

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
            }
            let s3_seqno = S3SeqNo::from(&seqno);
            let res = self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(self.build_s3_key(key, &s3_seqno))
                .send()
                .await;
            if let Err(e) = res {
                if e.as_service_error()
                    .is_some_and(|svc_err| svc_err.is_not_found())
                {
                    return Ok(CaSResult::ExpectationMismatch);
                }
                return Err(anyhow::Error::msg(e).into());
            }
        }
        let body = SdkBody::from(new.data);
        let new_s3_seqno = S3SeqNo::from(&new.seqno);
        let _res = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.build_s3_key(&key, &new_s3_seqno))
            .body(ByteStream::new(body))
            .if_none_match("*") // condition that states the object should not exist
            .send()
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(CaSResult::Committed)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let mut s3_seqno_list = Vec::with_capacity(1000.min(limit));
        self.s3_list(format!("{}/{}/", &self.prefix, &key), |res| {
            let next_page: Vec<_> = res
                .contents()
                .iter()
                .filter_map(|entry| {
                    entry
                        .key()
                        .map(|k| self.parse_s3_key(k).1)
                        .filter(|s3_seqno| <&S3SeqNo as Into<SeqNo>>::into(s3_seqno) >= from)
                })
                .collect();
            if next_page.is_empty() {
                return Ok(false);
            }
            // If we find that scan is often called with a seqno that has a significant number of
            // newer seqno and a small limit, it would mean that this vec is unecessarily large.
            // We could convert to a VecDeq and drain from the front in each iteration here to maintain
            // "limit" entries in agg.
            s3_seqno_list.extend(next_page);

            Ok(true)
        })
        .await?;

        // the s3 listing is newest first (desc order), but scan must return in asc order
        let mut results = Vec::with_capacity(limit.min(s3_seqno_list.len()));
        while let Some(s3_seqno) = s3_seqno_list.pop() {
            let res = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(self.build_s3_key(key, &s3_seqno))
                .send()
                .await
                .map_err(anyhow::Error::msg)?;
            results.push(VersionedData {
                seqno: (&s3_seqno).into(),
                data: res
                    .body
                    .collect()
                    .await
                    .map_err(anyhow::Error::msg)?
                    .into_bytes(),
            });
            if results.len() == limit {
                break;
            }
        }
        Ok(results)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let mut s3_seqno_to_delete = vec![];
        let mut greater_seq_exists = false;

        self.s3_list(format!("{}/{}/", &self.prefix, key), |res| {
            for entry in res.contents().iter() {
                if let Some(s3_seqno) = entry.key().map(|k| self.parse_s3_key(k).1) {
                    let found_seqno: SeqNo = (&s3_seqno).into();
                    if found_seqno < seqno {
                        s3_seqno_to_delete.push(s3_seqno);
                    } else {
                        greater_seq_exists = true;
                    }
                }
            }
            // because the s3 listing is in desc order, if a max_seqno hasn't been found in the first page,
            // there isn't a sequence number >= the provided seqno. Based on the other implementations, this
            // condition is an error.
            if !greater_seq_exists {
                return Err(anyhow!("No seqno >= {:?}", seqno));
            }
            Ok(true)
        })
        .await?;

        let num_deletes = s3_seqno_to_delete.len();
        for chunk in s3_seqno_to_delete.chunks(1000) {
            let mut delete = Delete::builder().quiet(true);
            for s3_seqno in chunk {
                let s3_obj_id = ObjectIdentifier::builder()
                    .key(self.build_s3_key(key, s3_seqno))
                    .build()
                    .expect("object identifier build");
                delete = delete.objects(s3_obj_id);
            }
            let delete = delete.build().expect("delete payload");
            let res = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(anyhow::Error::msg)?;
            // delete from oldest to newest, if any delete fails, this entire call fails.
            // unfortunately it means that we've deleted some of the data, but not all of it.
            if !res.errors().is_empty() {
                // log the errors, but only return the last one
                let err = res
                    .errors()
                    .last()
                    .unwrap()
                    .message()
                    .unwrap_or("S3 DeleteObject error")
                    .to_string();
                return Err(anyhow::Error::msg(err).into());
            }
        }
        Ok(num_deletes)
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::blob_impl_test;
    use crate::location::tests::consensus_impl_test;
    use tracing::info;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5586
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[ignore] // TODO: Reenable against minio so it can run locally
    async fn s3_blob() -> Result<(), ExternalError> {
        let config = match S3BlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    S3BlobConfig::EXTERNAL_TESTS_S3_BUCKET
                );
                return Ok(());
            }
        };
        let config_multipart = config.clone_with_new_uuid_prefix();

        blob_impl_test(move |path| {
            let path = path.to_owned();
            let config = config.clone();
            async move {
                let config = S3BlobConfig {
                    metrics: config.metrics.clone(),
                    client: config.client.clone(),
                    bucket: config.bucket.clone(),
                    prefix: format!("{}/s3_blob_impl_test/{}", config.prefix, path),
                    cfg: Arc::new(
                        ConfigSet::default()
                            .add(&ENABLE_S3_LGALLOC_CC_SIZES)
                            .add(&ENABLE_S3_LGALLOC_NONCC_SIZES),
                    ),
                    is_cc_active: true,
                };
                let mut blob = S3Blob::open(config).await?;
                blob.max_keys = 2;
                Ok(blob)
            }
        })
        .await?;

        // Also specifically test multipart. S3 requires all parts but the last
        // to be at least 5MB, which we don't want to do from a test, so this
        // uses the multipart code path but only writes a single part.
        {
            let blob = S3Blob::open(config_multipart).await?;
            blob.set_multi_part("multipart", "foobar".into()).await?;
            assert_eq!(
                blob.get("multipart").await?,
                Some(b"foobar".to_vec().into())
            );
        }

        Ok(())
    }

    #[mz_ore::test]
    fn should_multipart() {
        let config = MultipartConfig::default();
        assert_eq!(config.should_multipart(0), Ok(false));
        assert_eq!(config.should_multipart(1), Ok(false));
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD),
            Ok(false)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD + 1),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD * 2),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::MAX_SINGLE_UPLOAD_SIZE),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::MAX_SINGLE_UPLOAD_SIZE + 1),
            Err(
                "S3 does not support blobs larger than 5497558138880 bytes got: 5497558138881"
                    .into()
            )
        );
    }

    #[mz_ore::test]
    fn multipart_iter() {
        let iter = MultipartChunkIter::new(10, 0);
        assert_eq!(iter.collect::<Vec<_>>(), vec![]);

        let iter = MultipartChunkIter::new(10, 9);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..9)]);

        let iter = MultipartChunkIter::new(10, 10);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10)]);

        let iter = MultipartChunkIter::new(10, 11);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..11)]);

        let iter = MultipartChunkIter::new(10, 19);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..19)]);

        let iter = MultipartChunkIter::new(10, 20);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..20)]);

        let iter = MultipartChunkIter::new(10, 21);
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![(1, 0..10), (2, 10..20), (3, 20..21)]
        );
    }

    #[mz_ore::test]
    fn test_s3_seqno_ordering() {
        for range in [0..100, u64::MAX - 100..u64::MAX] {
            let data = range
                .map(|v| SeqNo(v))
                .map(|seqno| (seqno, S3SeqNo::from(&seqno).to_string()))
                .collect::<BTreeMap<_, _>>();
            assert!(data.values().rev().is_sorted());
            for (seqno, s3_seqno_str) in data.iter() {
                let converted: SeqNo = (&s3_seqno_str.parse::<S3SeqNo>().unwrap()).into();
                assert_eq!(*seqno, converted);
            }
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn s3_consensus() -> Result<(), ExternalError> {
        let config = S3ConsensusConfig::new_for_test().await?;
        consensus_impl_test(|| S3Consensus::new(config.clone())).await?;
        Ok(())
    }
}
