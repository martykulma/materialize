#!/usr/bin/env python3
"""
Load generator for upstream PostgreSQL tables created by setup_consensus_test.sh.

Each table has the schema:
    foo_N (id int, data text, ts timestamp without time zone)

Usage:
    python3 load_pg_tables.py [OPTIONS]

Options:
    --host HOST          PostgreSQL host (default: localhost)
    --port PORT          PostgreSQL port (default: 15432)
    --user USER          PostgreSQL user (default: postgres)
    --password PASSWORD  PostgreSQL password (default: postgres)
    --dbname DBNAME      Database name (default: test)
    --tables N           Number of tables (default: 600)
    --batch-size N       Tables to update per tick (default: 50)
    --rows-per-table N   Rows to update per table per tick (default: 1)
    --interval SECONDS   Seconds between ticks (default: 1.0)
    --mode MODE          Write mode: update, insert, mixed (default: update)
"""

import argparse
import hashlib
import os
import random
import sys
import time

import psycopg2
import psycopg2.extras


def parse_args():
    p = argparse.ArgumentParser(description="Load generator for PG source tables")
    p.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "15432")))
    p.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    p.add_argument("--password", default=os.environ.get("PGPASSWORD", "postgres"))
    p.add_argument("--dbname", default=os.environ.get("PGDATABASE", "test"))
    p.add_argument("--tables", type=int, default=600)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--rows-per-table", type=int, default=1)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument(
        "--mode",
        choices=["update", "insert", "mixed"],
        default="update",
    )
    return p.parse_args()


def connect(args):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )


def do_updates(cur, tables, rows_per_table):
    for t in tables:
        for _ in range(rows_per_table):
            row_id = random.randint(1, 1000)
            data = hashlib.md5(f"{time.time()}:{t}:{row_id}".encode()).hexdigest()
            cur.execute(
                f"UPDATE foo_{t} SET data = %s, ts = now() WHERE id = %s",
                (data, row_id),
            )


def do_inserts(cur, tables, rows_per_table):
    for t in tables:
        for _ in range(rows_per_table):
            row_id = random.randint(1001, 100000)
            data = hashlib.md5(f"{time.time()}:{t}:{row_id}".encode()).hexdigest()
            cur.execute(
                f"INSERT INTO foo_{t} (id, data, ts) VALUES (%s, %s, now())",
                (row_id, data),
            )


def do_mixed(cur, tables, rows_per_table):
    for t in tables:
        for _ in range(rows_per_table):
            if random.random() < 0.5:
                row_id = random.randint(1, 1000)
                data = hashlib.md5(f"{time.time()}:{t}:{row_id}".encode()).hexdigest()
                cur.execute(
                    f"UPDATE foo_{t} SET data = %s, ts = now() WHERE id = %s",
                    (data, row_id),
                )
            else:
                row_id = random.randint(1001, 100000)
                data = hashlib.md5(f"{time.time()}:{t}:{row_id}".encode()).hexdigest()
                cur.execute(
                    f"INSERT INTO foo_{t} (id, data, ts) VALUES (%s, %s, now())",
                    (row_id, data),
                )


MODES = {
    "update": do_updates,
    "insert": do_inserts,
    "mixed": do_mixed,
}


def main():
    args = parse_args()
    conn = connect(args)
    conn.autocommit = False

    all_tables = list(range(1, args.tables + 1))
    write_fn = MODES[args.mode]
    tick = 0
    offset = 0

    print(
        f"Starting load: {args.tables} tables, batch_size={args.batch_size}, "
        f"rows_per_table={args.rows_per_table}, interval={args.interval}s, mode={args.mode}"
    )

    try:
        while True:
            t0 = time.monotonic()

            # Round-robin through tables in batch_size chunks.
            batch = []
            for _ in range(args.batch_size):
                batch.append(all_tables[offset % len(all_tables)])
                offset += 1

            try:
                with conn.cursor() as cur:
                    write_fn(cur, batch, args.rows_per_table)
                conn.commit()
            except psycopg2.Error as e:
                print(f"tick {tick}: error: {e}", file=sys.stderr)
                conn.rollback()
                # Reconnect on fatal errors.
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect(args)
                conn.autocommit = False

            elapsed = time.monotonic() - t0
            tick += 1

            if tick % 10 == 0:
                tables_touched = min(offset, len(all_tables))
                print(
                    f"tick {tick}: {args.batch_size} tables in {elapsed:.3f}s, "
                    f"{tables_touched}/{len(all_tables)} tables covered this cycle"
                )

            sleep_for = args.interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print(f"\nStopped after {tick} ticks")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
