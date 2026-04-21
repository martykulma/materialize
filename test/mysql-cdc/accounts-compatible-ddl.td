# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

#
# Exercise upstream DDL that SHOULD remain compatible with the positional
# binlog decoder used on v26.19.0, against the same 12-column `accounts`
# layout as varchar-prefix-boundary.td (mixed utf8mb3/utf8mb4 VARCHARs).
# Every ALTER that supports it uses ALGORITHM=INSTANT, so the physical
# row layout diverges from the logical schema — the exact condition that
# has been suspected of causing positional-decoder drift against the
# customer's production table.
#
# Compatibility rule (src/mysql-util/src/desc.rs:78-130): `self.columns`
# must be a strict prefix of `other.columns`, with each position 1:1
# type-compatible, and all tracked keys still present. So:
#
#   * `ADD COLUMN ... (no AFTER / no FIRST)` at the tail  — COMPATIBLE
#   * `ADD COLUMN` with a DEFAULT or NOT NULL, still at tail — COMPATIBLE
#   * Tightening nullability from nullable → NOT NULL       — COMPATIBLE
#   * `DROP COLUMN` of an untracked trailing column         — COMPATIBLE
#
# INSTANT DDL note (MySQL 8.0.12+ for ADD at tail, 8.0.29+ for anywhere
# and for DROP): the server records the new/dropped columns in table
# metadata without rewriting existing rows. Existing rows keep their old
# physical layout; new rows are written in the new layout. Binlog ROWS
# events are supposed to emit the CURRENT logical schema, but any drift
# between that and what mysql_common parses would surface here.
#
# The binlog ROWS events after each ALTER carry one or more extra
# trailing column values. The positional decoder in
# src/mysql-util/src/decoding.rs:35 handles them via
# `EitherOrBoth::Right(_) => break;`, silently dropping the trailing
# values once the cached descriptor's columns are exhausted.
#
# We then do INSERT, UPDATE, and DELETE after each ALTER to exercise all
# row-event variants (Write_rows, Update_rows, Delete_rows). An UPDATE of
# a row whose before-image was written under the old schema is
# particularly useful: the server rewrites the before-image into the
# current logical schema, so the binlog event has the new trailing
# column(s) on both sides of the update, and the retraction's error (or
# success) bytes must match the insertion's for consolidation to work.
#
# testdrive NOTE: mysql-execute body treats each line as a separate
# query — every statement below is kept on a single line even when long.
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
# Strict mode off — matches production RDS defaults and keeps us from
# being derailed by values that would otherwise get rejected.
SET SESSION sql_mode = '';
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE accounts (id int unsigned NOT NULL AUTO_INCREMENT, title_id int unsigned DEFAULT NULL, country_id int unsigned NOT NULL DEFAULT '243', email varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, alternate_email varchar(80) COLLATE utf8mb3_unicode_ci DEFAULT NULL, password varchar(100) COLLATE utf8mb3_unicode_ci NOT NULL, first_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, middle_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, last_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, display_name varchar(100) COLLATE utf8mb3_unicode_ci DEFAULT NULL, profile_image varchar(80) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '', language varchar(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en', PRIMARY KEY (id)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;
# Row 1 — pre-snapshot baseline.
INSERT INTO accounts (email, password, first_name, last_name, display_name, language) VALUES ('alice@example.com', 'hunter2', 'Alice', 'Smith', 'alice', 'en');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE public.accounts) WITH (EXCLUDE COLUMNS (password));

> SELECT id, email, language FROM accounts;
1 "alice@example.com" en

#
# DDL #1 — ADD COLUMN at the end using ALGORITHM=INSTANT. Only metadata
# is changed upstream; existing rows keep their old physical layout.
# Binlog ROWS events for subsequent writes carry 13 logical values; the
# decoder drops the 13th via EitherOrBoth::Right.
#
$ mysql-execute name=mysql
ALTER TABLE accounts ADD COLUMN created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, ALGORITHM=INSTANT;
INSERT INTO accounts (email, password, first_name, last_name, display_name, language) VALUES ('bob@example.com', 's3cret', 'Bob', 'Jones', 'bob', 'fr');
UPDATE accounts SET language = 'es' WHERE id = 1;
DELETE FROM accounts WHERE email = 'alice@example.com' AND language = 'en';

# Only Bob should be here with language 'fr', and Alice's language must be 'es'
# — but the DELETE's `language = 'en'` predicate no longer matches because the
# UPDATE already changed it. So Alice (id=1, 'es') and Bob (id=2, 'fr'), 2 rows.
> SELECT id, email, language FROM accounts ORDER BY id;
1 "alice@example.com" es
2 "bob@example.com" fr

#
# DDL #2 — ADD multiple columns at the end in one INSTANT ALTER. After
# this, the on-disk "old" rows (Alice, Bob) are in the 12-col physical
# layout while "new" rows written from here on are in a different physical
# layout that includes these trailing defaults. The binlog still emits
# the logical row; the decoder drops positions beyond 12.
#
$ mysql-execute name=mysql
ALTER TABLE accounts ADD COLUMN modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, ADD COLUMN deleted datetime DEFAULT NULL, ALGORITHM=INSTANT;
INSERT INTO accounts (email, password, first_name, last_name, language) VALUES ('carol@example.com', 'pw', 'Carol', 'Chen', 'zh');
INSERT INTO accounts (email, password, first_name, last_name, language) VALUES ('dave@example.com', 'pw', 'Dave', 'Dahl', 'de-DE');

> SELECT id, email, language FROM accounts ORDER BY id;
1 "alice@example.com" es
2 "bob@example.com" fr
3 "carol@example.com" zh
4 "dave@example.com" "de-DE"

#
# DDL #3 — UPDATE that touches a row inserted before both ALTERs. The
# before-image is emitted under the CURRENT logical schema (15 columns
# now), so both before- and after-images have to decode to the same
# 12-column prefix for the retraction to cancel the prior insertion.
#
$ mysql-execute name=mysql
UPDATE accounts SET display_name = 'alice-updated', language = 'en-US' WHERE id = 1;

> SELECT id, email, display_name, language FROM accounts WHERE id = 1;
1 "alice@example.com" "alice-updated" "en-US"

#
# DDL #4 — tighten nullability on a tracked column from nullable → NOT NULL.
# Per is_compatible() in src/mysql-util/src/desc.rs:282, self (nullable)
# matched against other (NOT NULL) is compatible because self is "more
# permissive". No backfill needed because `title_id` has no NULLs.
#
# NOTE: INSTANT cannot be used for a nullability change — the docs list
# ADD COLUMN / DROP COLUMN / RENAME / default-change only. INPLACE also
# refuses under sql_mode='' (non-strict), so we leave the algorithm
# unspecified and let MySQL pick. This is the only ALTER in the test
# that is not INSTANT; all the rest exercise the instant-DDL code path.
#
$ mysql-execute name=mysql
UPDATE accounts SET title_id = 0 WHERE title_id IS NULL;
ALTER TABLE accounts MODIFY COLUMN title_id int unsigned NOT NULL DEFAULT 0;
INSERT INTO accounts (email, password, first_name, title_id, language) VALUES ('eve@example.com', 'pw', 'Eve', 7, 'jp');

> SELECT id, email, title_id, language FROM accounts ORDER BY id;
1 "alice@example.com" 0 "en-US"
2 "bob@example.com" 0 fr
3 "carol@example.com" 0 zh
4 "dave@example.com" 0 "de-DE"
5 "eve@example.com" 7 jp

#
# DDL #5 — multiple trailing adds via INSTANT plus data that crosses the
# 255-byte length-prefix boundary on `password` (100 × 中 = 300 bytes).
# Combines INSTANT schema evolution with the length-prefix stress case
# from varchar-prefix-boundary.td.
#
$ mysql-execute name=mysql
ALTER TABLE accounts ADD COLUMN login_count int NOT NULL DEFAULT 0, ADD COLUMN tags varchar(200) COLLATE utf8mb3_unicode_ci DEFAULT NULL, ALGORITHM=INSTANT;
INSERT INTO accounts (email, password, first_name, language) VALUES ('frank@example.com', REPEAT('中', 100), 'Frank', 'zh');
UPDATE accounts SET password = REPEAT('中', 85) WHERE id = 5;

# `password` is excluded from Materialize per the customer's EXCLUDE
# COLUMNS config — still appears in the binlog row event but is consumed
# without being decoded into the subsource. We verify the surviving
# columns look right; any drift caused by mis-handling the excluded
# slot would show here as shifted language values or email mismatches.
> SELECT id, email, language FROM accounts ORDER BY id;
1 "alice@example.com" "en-US"
2 "bob@example.com" fr
3 "carol@example.com" zh
4 "dave@example.com" "de-DE"
5 "eve@example.com" jp
6 "frank@example.com" zh

#
# DDL #6 — INSTANT DROP COLUMN on an untracked trailing column (MySQL
# 8.0.29+). This is the DDL that most directly creates a divergence
# between logical and physical row layout: every existing row still has
# `tags` in its physical storage, but the logical schema no longer
# includes it. Binlog ROWS events for subsequent writes are supposed to
# emit 15 logical columns; anything that leaks the physical 16th slot is
# drift. `tags` is beyond Materialize's 12-column prefix, so the DROP is
# compatible with the schema check.
#
$ mysql-execute name=mysql
ALTER TABLE accounts DROP COLUMN tags, ALGORITHM=INSTANT;
INSERT INTO accounts (email, password, first_name, language) VALUES ('grace@example.com', 'pw', 'Grace', 'ko');
UPDATE accounts SET language = 'pt' WHERE id = 6;
DELETE FROM accounts WHERE id = 2;

#
# Sanity: `language` must stay within 5 characters across every row
# regardless of how many times the upstream schema has been extended
# and shrunk via INSTANT DDL. Any value over 5 characters or otherwise
# corrupted means the positional decoder lost its place — either because
# a trailing column leaked through or because a DROPped column's
# physical residue made its way into the binlog.
#
> SELECT id, language, char_length(language), octet_length(language) FROM accounts ORDER BY id;
1 "en-US" 5 5
3 zh 2 2
4 "de-DE" 5 5
5 jp 2 2
6 pt 2 2
7 ko 2 2

# And one negative-case reminder of what compatibility does NOT allow
# (kept as a comment, not asserted — these would stall the source even
# under ALGORITHM=INSTANT):
#
#   ALTER TABLE accounts ADD COLUMN note VARCHAR(20) AFTER id, ALGORITHM=INSTANT;  -- AFTER/FIRST: stalls
#   ALTER TABLE accounts DROP COLUMN language, ALGORITHM=INSTANT;                  -- DROP tracked: stalls
#   ALTER TABLE accounts RENAME COLUMN language TO lang, ALGORITHM=INSTANT;        -- rename tracked: stalls
#   ALTER TABLE accounts MODIFY language VARCHAR(10);                              -- retype tracked: stalls

> DROP SOURCE mz_source CASCADE;
