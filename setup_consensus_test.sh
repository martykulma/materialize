#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


pg_image=postgres:18
sources=1
tables_per_source=900
total=$(($sources * $tables_per_source))

pg_src_container_name="consensus-pg-src"
pg_src_port=15432
pg_src_user=postgres
pg_src_pw=postgres
pg_src_db=test
pg_src_pub=mzpub

mz_sql_file="mz_$$.sql"
pg_sql_file="pg_$$.sql"
cleanup() {
    rm -f "$mz_sql_file" "$pg_sql_file"
}
trap cleanup EXIT


gen_mz_sql() {
    local ti=1
    echo "CREATE SECRET pgpass AS '$pg_src_pw';"
    echo "CREATE CONNECTION pgcon TO POSTGRES (HOST 'localhost', USER '$pg_src_user', DATABASE '$pg_src_db', PORT $pg_src_port, PASSWORD SECRET pgpass);"
    for i in $(seq 1 $sources) ; do
        echo "CREATE CLUSTER sut$i (SIZE = 'scale=1,workers=32');"
        echo "BEGIN;"
        echo "CREATE SOURCE pg_src_$i IN CLUSTER sut$i FROM POSTGRES CONNECTION pgcon (PUBLICATION '$pg_src_pub');"
        for j in $(seq 1 $tables_per_source) ; do
            echo "CREATE TABLE foo_$ti FROM SOURCE pg_src_$i (REFERENCE foo_$ti);"
            ti=$(($ti+1))
        done
        echo "COMMIT;"
    done
}

gen_pg_sql() {
    echo "DROP DATABASE IF EXISTS $pg_src_db;"
    echo "CREATE DATABASE $pg_src_db;"
    echo "\c test";

    for i in $(seq 1 $total) ; do
        echo "CREATE TABLE foo_$i (id int, data text, ts timestamp without time zone);"
        echo "INSERT INTO foo_$i SELECT i, sha512(i::text::bytea)::text FROM generate_series(1,1000) AS i;"
        echo "ALTER TABLE foo_$i REPLICA IDENTITY FULL;"
    done

    echo ""
    echo "CREATE PUBLICATION $pg_src_pub FOR TABLES IN SCHEMA public;"
}


#### MAIN



echo "Generating SQL for PG to $pg_sql_file"
gen_pg_sql > $pg_sql_file

echo "Generating SQL for Materialize to $mz_sql_file"
gen_mz_sql > $mz_sql_file



echo "Stopping $pg_src_container_name if it happens to be running"
docker stop "$pg_src_container_name" 2>/dev/null

echo "Starting PG source container $pg_src_container_name on port $pg_src_port"
docker run --rm -d \
    -p $pg_src_port:5432 \
    --name "$pg_src_container_name" \
    -e POSTGRES_PASSWORD="$pg_src_pw" \
    $pg_image \
    -c wal_level=logical


echo "waiting a few seconds for the conatiner to start"
until docker run --rm $pg_image pg_isready -h host.docker.internal -U postgres -p "$pg_src_port" -q ; do
    sleep 1
done

echo "Setting up PG source"
docker run --rm \
    -v $(pwd):/scripts \
    -e PGPASSWORD="$pg_src_pw" \
    -e PGDATABASE="postgres" \
    -e PGUSER="postgres" \
    -e PGPORT="$pg_src_port" \
    $pg_image \
    psql -h host.docker.internal -f /scripts/"$pg_sql_file"


envd="bin/environmentd"
envd_params="--release --reset -- --all-features --unsafe-mode"

echo "start Materialize using $envd $envd_params >envd.log 2>envd.err.log"
$envd $envd_params >envd.log 2>envd.err.log &
envd_pid=$!


echo "waiting for Materialize to come online -- this might take a while if it has to compile!"
SECONDS=0
until docker run --rm $pg_image pg_isready -h host.docker.internal -U materialize -p 6875 -q ; do
    if ! kill -0 $envd_pid >/dev/null ; then
        echo "environmentd exited, check envd.err.log" >&2
        exit 1
    fi

    printf "\r\033[KStill waiting... %d seconds" $SECONDS
    sleep 1
done

echo "creating source in materialize"
mz_db="materialize"
mz_port="6875"


docker run --rm \
    -v $(pwd):/scripts \
    -e PGUSER="materialize" \
    -e PGPASSWORD="" \
    -e PGDATABASE="$mz_db" \
    -e PGPORT="$mz_port" \
    $pg_image \
    psql -h host.docker.internal -f /scripts/"$mz_sql_file"


echo ""
echo "psql postgres://materialize@localhost:6875/materialize"

# leave bin/environmentd running
disown $envd_pid
