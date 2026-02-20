// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC client and proto definitions for the persist-consensus service.
//!
//! This crate provides:
//! - Generated protobuf types for the `PersistConsensusService` gRPC API.
//! - [`GrpcConsensusClient`](client::GrpcConsensusClient): a `Consensus` trait
//!   implementation that talks to a persist-consensus server over gRPC.

pub mod client;
pub mod generated;
