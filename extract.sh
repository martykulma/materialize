#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


echo "grabbing list of shard + seqno that conflicted"
grep -o 'MISMATCH.*' envd.err.log | grep -o "key=.*" | sort -u > /tmp/mismatch.txt

total=$(wc -l /tmp/mismatch.txt | awk '{print $1}')

output_file=cas_conflicts.txt
echo "extracting to $output_file"
echo "----" > $output_file

count=0
while IFS= read -r line ; do
    (( count++ ))
    grep -w "$line" envd.err.log >> $output_file
    echo "----" >> $output_file
    printf "\r\033[K%d%%" $((100 * $count / $total))
done < /tmp/mismatch.txt
