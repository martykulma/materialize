# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that ALTER SINK ... SET FROM accepts a relation whose schema is
# completely different from the original (different column names, count, and
# types) when the sink uses FORMAT JSON without a schema registry, since there
# is no external compatibility check to enforce schema equality.

$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE TABLE before_alter (id int NOT NULL, name string);
> INSERT INTO before_alter VALUES (1, 'fish');

> CREATE TABLE after_alter (
    score double precision,
    active boolean,
    label string NOT NULL,
    payload jsonb
  );
# This row predates the ALTER and should not appear in the sink output.
> INSERT INTO after_alter VALUES (0.0, false, 'ignored', '{}'::jsonb);

> CREATE SINK json_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM before_alter
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-json-different-schema-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;

$ kafka-verify-data format=json sink=materialize.public.json_sink key=false
{"before": null, "after": {"id": 1, "name": "fish"}}

> ALTER SINK json_sink SET FROM after_alter;

# The sink will start sinking updates from `after_alter` at the timestamp that
# the previous dataflow happens to stop. This happens pretty quickly but we
# wait a few seconds more for good measure to avoid flaking.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s

> INSERT INTO after_alter VALUES (3.14, true, 'pi', '{"k": "v"}'::jsonb);

$ kafka-verify-data format=json sink=materialize.public.json_sink key=false
{"before": null, "after": {"score": 3.14, "active": true, "label": "pi", "payload": {"k": "v"}}}

# The sink should be running healthily, not stuck in an error state.
> SELECT status FROM mz_internal.mz_sink_statuses st
  JOIN mz_sinks s ON s.id = st.id
  WHERE s.name = 'json_sink';
running
