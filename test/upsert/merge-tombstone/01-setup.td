# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for consolidating_merge_function silently dropping
# Value(tombstone) entries during RocksDB compaction/merge.
#
# The bug: during snapshot ingestion with prevent_snapshot_buffering=true,
# the AtTime drain writes provisional Value entries (including tombstones)
# via multi_put. Later, persist feedback writes Consolidating merge operands
# for the same keys via multi_merge. When RocksDB merges these, the merge
# function silently drops Value(tombstone) entries because into_finalized_value()
# returns None for tombstones, causing the if-let to skip the entry.
# This leads to incorrect diff_sum in the merged Consolidating state.
#
# Strategy:
# 1. Pre-populate the Kafka topic with interleaved inserts and deletes
#    spread across enough data to create multiple RocksDB LSM levels
# 2. Create the upsert source (with merge operator enabled)
# 3. During snapshot ingestion, AtTime drains write Value(tombstone) via multi_put
# 4. Persist feedback writes Consolidating merge operands for the same keys
# 5. RocksDB compaction merges Value(tombstone) + Consolidating → bug

$ kafka-create-topic topic=merge-tombstone partitions=1

> CREATE CLUSTER storage_cluster SIZE 'bootstrap'

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT
