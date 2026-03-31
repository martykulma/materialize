# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Delete keys by sending null values (empty value after key-terminator).
# These become Value(tombstone) entries in RocksDB via multi_put.
$ kafka-ingest format=bytes topic=merge-tombstone key-format=bytes key-terminator=: repeat=${arg.repeat} start-iteration=${arg.start}
"${kafka-ingest.iteration}":
