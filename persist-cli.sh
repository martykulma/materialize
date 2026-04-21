#!/bin/bash

# AWS creds scoped to the customer environment's bucket (on-call profile)
export AWS_PROFILE=mz-cloud-production-engineering-on-call
export AWS_REGION=us-west-2

export CONSENSUS_URI="postgres://marty:lN31g0tYEJmkp9HvsbIHjA@pa-us-west-2-0-mdh.aws-us-west-2.cockroachlabs.cloud:26257/environment_de9334b7254e42fcb849681caed4049b0?sslmode=verify-full&options=--search_path=consensus&sslrootcert=$HOME/Library/CockroachCloud/certs/2ba885da-330b-4151-9432-acd16365dd43/pa-us-west-2-0-ca.crt"
export BLOB_URI="s3://mzcloud-production-us-west-2-persistence-8ddcd0d3/system:serviceaccount:environment-de9334b7-254e-42fc-b849-681caed4049b-0:environment-de9334b7-254e-42fc-b849-681caed4049b-0"
export SHARD='s783e95c2-c508-416a-b698-4b6cad2267a1'


export PERSISTCLI='./target/release/persistcli'
$PERSISTCLI inspect state \
--shard-id $SHARD \
--consensus-uri "$CONSENSUS_URI" \
> /tmp/state.json




jq -r '[.. | objects | .kind? // empty | select(has("Key")) | .Key] | unique[]' /tmp/state.json \
| while read key; do
  $PERSISTCLI inspect blob-batch-part --shard-id $SHARD --blob-uri "$BLOB_URI" --key "$key"
  v=$($PERSISTCLI inspect blob-batch-part \
        --shard-id $SHARD \
        --blob-uri "$BLOB_URI" \
        --key "$key" \
      | jq -r '.updates[] | select(.v | contains("source must be dropped")) | .v' | head -1)
  if [ -n "$v" ]; then
    echo "--- found in blob key: $key ---"
    echo "$v"
    break
  fi
done
