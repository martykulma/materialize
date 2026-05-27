// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Glue Schema Registry client.

use std::fmt;

use aws_sdk_glue::error::{DisplayErrorContext, SdkError};
use aws_sdk_glue::operation::get_registry::GetRegistryError as SdkGetRegistryError;
use aws_sdk_glue::operation::get_schema_version::GetSchemaVersionError as SdkGetSchemaVersionError;
use aws_sdk_glue::types::{
    RegistryId, RegistryStatus as SdkRegistryStatus, SchemaVersionStatus as SdkSchemaVersionStatus,
};
use aws_types::SdkConfig;
use thiserror::Error;
use uuid::Uuid;

/// An API client for the AWS Glue Schema Registry.
///
/// `Client` is cheap to clone — internally it wraps an [`aws_sdk_glue::Client`],
/// which is itself a clone-friendly handle backed by a shared connection pool.
#[derive(Clone)]
pub struct Client {
    inner: aws_sdk_glue::Client,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Client {
    pub(crate) fn from_sdk_config(sdk_config: SdkConfig) -> Self {
        Client {
            inner: aws_sdk_glue::Client::new(&sdk_config),
        }
    }

    /// Look up a registry by name.
    ///
    /// Returns [`GetRegistryError::NotFound`] if the registry does not exist
    /// in the configured account and region. Other errors (auth failures,
    /// throttling, transport) surface as [`GetRegistryError::Other`].
    pub async fn get_registry(&self, name: &str) -> Result<Registry, GetRegistryError> {
        let id = RegistryId::builder().registry_name(name).build();
        let output = self
            .inner
            .get_registry()
            .registry_id(id)
            .send()
            .await
            .map_err(classify_get_registry_error)?;
        Ok(Registry {
            name: output.registry_name,
            arn: output.registry_arn,
            description: output.description,
            lifecycle_status: output.status.map(RegistryLifecycleStatus::from_sdk),
        })
    }

    /// Fetch a schema version by its UUID.
    ///
    /// This is the source-decode path: the UUID is read from the Glue
    /// wire-format header, and the returned `SchemaVersion::definition`
    /// carries the writer schema (Avro JSON, for our usage).
    ///
    /// Glue schema-version UUIDs are globally unique within an AWS account
    /// and this call does **not** scope to any registry — it returns the
    /// matching version from anywhere the configured credentials can read.
    /// Returns [`GetSchemaVersionError::NotFound`] only when no schema
    /// version with this UUID exists in any visible registry. Callers that
    /// need to enforce a specific registry must check the returned
    /// `SchemaArn` themselves.
    ///
    /// The AWS `GetSchemaVersion` API forces a choice between
    /// `SchemaVersionId` (UUID, registry-agnostic) **or**
    /// `SchemaId(RegistryName, SchemaName) + SchemaVersionNumber`; the
    /// two input modes are mutually exclusive and cannot be combined.
    /// See <https://docs.aws.amazon.com/glue/latest/webapi/API_GetSchemaVersion.html>:
    /// the `SchemaVersionId` field description states "Either this or the
    /// `SchemaId` wrapper has to be provided."
    pub async fn get_schema_version_by_id(
        &self,
        id: Uuid,
    ) -> Result<SchemaVersion, GetSchemaVersionError> {
        let output = self
            .inner
            .get_schema_version()
            .schema_version_id(id.to_string())
            .send()
            .await
            .map_err(|e| classify_get_schema_version_error(e, id))?;
        Ok(SchemaVersion {
            schema_version_id: output.schema_version_id,
            schema_arn: output.schema_arn,
            definition: output.schema_definition,
            data_format: output.data_format.map(|f| f.as_str().to_string()),
            version_number: output.version_number,
            lifecycle_status: output.status.map(SchemaVersionLifecycleStatus::from_sdk),
        })
    }
}

/// A Glue Schema Registry, as returned by [`Client::get_registry`].
///
/// Only the fields Materialize currently cares about are surfaced; the full
/// SDK type carries a few additional timestamps that we ignore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    pub name: Option<String>,
    pub arn: Option<String>,
    pub description: Option<String>,
    pub lifecycle_status: Option<RegistryLifecycleStatus>,
}

/// Lifecycle status of a Glue registry.
///
/// Mirrors `aws_sdk_glue::types::RegistryStatus`, with `Unknown(String)` as
/// the forward-compat escape hatch for variants AWS may add later. Keeping
/// our own enum means callers get exhaustive matching without taking a
/// direct dependency on the SDK type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryLifecycleStatus {
    Available,
    Deleting,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl RegistryLifecycleStatus {
    fn from_sdk(status: SdkRegistryStatus) -> Self {
        match &status {
            SdkRegistryStatus::Available => RegistryLifecycleStatus::Available,
            SdkRegistryStatus::Deleting => RegistryLifecycleStatus::Deleting,
            _ => RegistryLifecycleStatus::Unknown(status.as_str().to_string()),
        }
    }
}

/// Errors returned by [`Client::get_registry`].
#[derive(Debug, Error)]
pub enum GetRegistryError {
    /// The named registry does not exist in the configured account/region.
    /// Maps from Glue's `EntityNotFoundException`.
    #[error("registry not found")]
    NotFound,
    /// Anything else: auth failure, throttling, transport error, etc.
    /// The wrapped message preserves the upstream SDK's diagnostic.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_registry_error(err: SdkError<SdkGetRegistryError>) -> GetRegistryError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            SdkGetRegistryError::EntityNotFoundException(_)
        )
    {
        return GetRegistryError::NotFound;
    }
    GetRegistryError::Other(format!("{err}"))
}

/// A Glue schema version, as returned by [`Client::get_schema_version_by_id`].
///
/// `definition` is the format-specific schema text; for Avro it is a JSON
/// document the Avro parser can ingest directly. The remaining fields are
/// informational and exist for debug logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaVersion {
    pub schema_version_id: Option<String>,
    pub schema_arn: Option<String>,
    /// The format-specific schema text (Avro JSON, JSON Schema, etc.).
    pub definition: Option<String>,
    /// Glue's data-format tag — e.g. `"AVRO"` or `"JSON"`. Kept as a string
    /// rather than the SDK enum so the SDK type doesn't leak to callers.
    pub data_format: Option<String>,
    pub version_number: Option<i64>,
    pub lifecycle_status: Option<SchemaVersionLifecycleStatus>,
}

/// Lifecycle status of a Glue schema version.
///
/// Mirrors `aws_sdk_glue::types::SchemaVersionStatus`. See
/// [`RegistryLifecycleStatus`] for the rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaVersionLifecycleStatus {
    Available,
    Pending,
    Failure,
    Deleting,
    /// A value the SDK reported that this crate does not yet know about.
    Unknown(String),
}

impl SchemaVersionLifecycleStatus {
    fn from_sdk(status: SdkSchemaVersionStatus) -> Self {
        match &status {
            SdkSchemaVersionStatus::Available => SchemaVersionLifecycleStatus::Available,
            SdkSchemaVersionStatus::Pending => SchemaVersionLifecycleStatus::Pending,
            SdkSchemaVersionStatus::Failure => SchemaVersionLifecycleStatus::Failure,
            SdkSchemaVersionStatus::Deleting => SchemaVersionLifecycleStatus::Deleting,
            _ => SchemaVersionLifecycleStatus::Unknown(status.as_str().to_string()),
        }
    }
}

/// Errors returned by [`Client::get_schema_version_by_id`].
#[derive(Debug, Error)]
pub enum GetSchemaVersionError {
    /// No schema version exists for the supplied UUID in the configured
    /// account/region. Maps from Glue's `EntityNotFoundException`.
    #[error("schema version {0} not found")]
    NotFound(Uuid),
    /// Anything else: auth failure, throttling, transport error, etc.
    #[error("AWS Glue error: {0}")]
    Other(String),
}

fn classify_get_schema_version_error(
    err: SdkError<SdkGetSchemaVersionError>,
    id: Uuid,
) -> GetSchemaVersionError {
    if let SdkError::ServiceError(service_err) = &err
        && matches!(
            service_err.err(),
            SdkGetSchemaVersionError::EntityNotFoundException(_)
        )
    {
        return GetSchemaVersionError::NotFound(id);
    }
    GetSchemaVersionError::Other(DisplayErrorContext(&err).to_string())
}
