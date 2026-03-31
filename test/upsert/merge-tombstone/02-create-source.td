# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Create the source BEFORE populating data so that streaming inserts/deletes
# create the necessary progress events for AtTime drains.
> BEGIN
> CREATE SOURCE merge_tombstone_src
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-merge-tombstone-${testdrive.seed}')

> CREATE TABLE merge_tombstone FROM SOURCE merge_tombstone_src (REFERENCE "testdrive-merge-tombstone-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT
> COMMIT

> CREATE MATERIALIZED VIEW merge_tombstone_count AS SELECT COUNT(*) AS cnt FROM merge_tombstone
