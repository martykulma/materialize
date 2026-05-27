// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_debug_implementations)]

//! An API client for the [AWS Glue Schema Registry][gsr].
//!
//! This crate is the Glue analogue of [`mz-ccsr`], the Confluent Schema
//! Registry client. It is intentionally narrow: only the surface needed by
//! the staged Materialize integration is implemented. Endpoints land
//! alongside the feature that uses them.
//!
//! Currently implemented:
//!
//! * [`Client::get_registry`] — used by `GlueSchemaRegistryConnection::validate`
//!   to verify a registry exists at `CREATE CONNECTION` time.
//!
//! Future stages will add:
//!
//! * `get_schema_version_by_id` — source decode (Stage 4).
//! * `register_schema_version`, `get_schema_version_by_definition`,
//!   `get_compatibility`, `update_compatibility` — sink encode (Stage 5).
//!
//! ## Example usage
//!
//! ```no_run
//! # async {
//! use mz_aws_glue_schema_registry::{Client, ClientConfig};
//! use aws_types::SdkConfig;
//!
//! let sdk_config: SdkConfig = unimplemented!("from AwsConnection::load_sdk_config");
//! let client = ClientConfig::new(sdk_config).build();
//! let registry = client.get_registry("my-registry").await?;
//! # let _ = registry;
//! # Ok::<_, mz_aws_glue_schema_registry::GetRegistryError>(())
//! # };
//! ```
//!
//! [gsr]: https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html
//! [`mz-ccsr`]: https://docs.rs/mz-ccsr

mod client;
mod config;

pub use client::{Client, GetRegistryError, Registry};
pub use config::ClientConfig;
