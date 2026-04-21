# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=90s

#
# Full 23-column reproduction of a customer `accounts` table, vs the
# 12-column subset in accounts-compatible-ddl.td. The extra columns add:
#   * 3 ENUM columns (gender, theme, status) — encoded in binlog as
#     packed ints but declared as MYSQL_TYPE_STRING with ENUM flag in
#     TABLE_MAP metadata; worth exercising because under
#     binlog_row_metadata=minimal the optional enum-value-name arrays
#     are stripped and mysql_common relies on the required
#     column_types flag byte to identify ENUMs vs plain STRINGs.
#   * DATE column (birthdate).
#   * Three trailing DATETIME columns (created, modified, deleted).
#   * Secondary indexes including UNIQUE KEY on email, composite keys,
#     non-unique indexes — exercises the keys-must-match path in
#     determine_compatibility().
#
# Every ALTER uses ALGORITHM=INSTANT where MySQL allows it; nullability
# tightening uses the MySQL default (INSTANT isn't valid for that).
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
SET SESSION sql_mode = '';
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE accounts (id int unsigned NOT NULL AUTO_INCREMENT, title_id int unsigned DEFAULT NULL, country_id int unsigned NOT NULL DEFAULT '243', email varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, alternate_email varchar(80) COLLATE utf8mb3_unicode_ci DEFAULT NULL, password varchar(100) COLLATE utf8mb3_unicode_ci NOT NULL, first_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, middle_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, last_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, display_name varchar(100) COLLATE utf8mb3_unicode_ci DEFAULT NULL, profile_image varchar(80) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '', language varchar(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en', gender enum('male', 'female', 'unknown') COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'unknown', mobile_phone varchar(20) COLLATE utf8mb3_unicode_ci DEFAULT NULL, birthdate date DEFAULT NULL, id_phrase varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, admin int DEFAULT '0', verified int DEFAULT '0', theme enum('light', 'gray', 'dark', 'classic') COLLATE utf8mb3_unicode_ci DEFAULT 'dark', status enum('good', 'in-review', 'terminated', 'icr-level-1', 'icr-level-2', 'review-passed') CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT 'good', created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, deleted datetime DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY email_unique (email), KEY country_id (country_id), KEY email__password (email, password), KEY id_phrase (id_phrase), KEY accounts_created_idx (created), KEY title_id (title_id), KEY acc_id_deleted (id, deleted, admin), KEY acc_admin (admin)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;
# Row 1 — pre-snapshot baseline: default enum values, NULL nullable fields.
INSERT INTO accounts (email, password, first_name, last_name, display_name) VALUES ('alice@example.com', 'hunter2', 'Alice', 'Smith', 'alice');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE public.accounts) WITH (TEXT COLUMNS (gender, theme, status), EXCLUDE COLUMNS (password));

> SELECT id, email, language, gender, theme, status FROM accounts;
1 "alice@example.com" en unknown dark good

# Row 2 — exercise each ENUM at every possible value, plus every
# multibyte VARCHAR near its max, plus a DATE and a DATETIME(6)-ish
# timestamp (via the `created` NOT NULL DEFAULT CURRENT_TIMESTAMP).
# Also writes the `admin`/`verified` INTs explicitly.
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language, gender, mobile_phone, birthdate, id_phrase, admin, verified, theme, status) VALUES (CONCAT(REPEAT('a', 68), '@example.com'), CONCAT(REPEAT('a', 68), '@example.com'), REPEAT('x', 100), REPEAT('f', 40), REPEAT('m', 40), REPEAT('l', 40), REPEAT('d', 100), REPEAT('p', 80), 'en-US', 'female', '+1-555-0100', '1990-06-15', 'my-phrase', 1, 1, 'light', 'in-review');

# Row 3 — CJK across every VARCHAR; ENUMs walking through their next values.
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language, gender, mobile_phone, birthdate, id_phrase, theme, status) VALUES (CONCAT(REPEAT('中', 16), '@ex.com'), CONCAT(REPEAT('中', 16), '@ex.com'), REPEAT('中', 100), REPEAT('中', 40), NULL, REPEAT('中', 40), REPEAT('中', 100), REPEAT('中', 80), '中文', 'male', REPEAT('中', 6), '2000-02-29', REPEAT('中', 15), 'gray', 'terminated');

# Row 4 — emoji in utf8mb4 columns; ENUMs at their max ordinal; NULL
# nullable INTs to exercise the NULL bitmap.
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language, gender, mobile_phone, birthdate, id_phrase, admin, verified, theme, status) VALUES (REPEAT('🦀', 80), CONCAT(REPEAT('中', 16), '@ex.com'), REPEAT('中', 100), REPEAT('🦀', 40), REPEAT('🦀', 40), REPEAT('🦀', 40), REPEAT('中', 100), REPEAT('中', 80), 'zh', 'unknown', '+44 20 7946', '1970-01-01', 'phrase', NULL, NULL, 'dark', 'review-passed');

# Row 5 — exactly 255-byte password + display_name (85 × 中), tight
# VARCHAR(5)/VARCHAR(45) values, `deleted` datetime explicitly set.
$ mysql-execute name=mysql
INSERT INTO accounts (email, password, first_name, display_name, profile_image, language, gender, theme, status, created, modified, deleted) VALUES ('boundary@ex.com', REPEAT('中', 85), 'Boundary', REPEAT('中', 85), REPEAT('a', 80), 'zh', 'female', 'classic', 'icr-level-1', '2024-01-01 00:00:00', '2024-06-01 12:00:00', '2025-06-15 18:30:00');

# Row 6 — 256-byte password (crosses the 1-byte→2-byte prefix boundary),
# status at its 5th value.
$ mysql-execute name=mysql
INSERT INTO accounts (email, password, first_name, display_name, profile_image, language, status) VALUES ('over255@ex.com', CONCAT(REPEAT('中', 85), 'a'), 'Over', CONCAT(REPEAT('中', 85), 'a'), REPEAT('a', 80), 'en', 'icr-level-2');

# Wait for the snapshot + all 6 rows to propagate.
> SELECT id, email, language, gender, theme, status FROM accounts ORDER BY id;
1 "alice@example.com" en unknown dark good
2 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@example.com" "en-US" female light "in-review"
3 "中中中中中中中中中中中中中中中中@ex.com" "中文" male gray terminated
4 "🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀" zh unknown dark "review-passed"
5 "boundary@ex.com" zh female classic "icr-level-1"
6 "over255@ex.com" en unknown dark "icr-level-2"

# Lengths cross-check on the remaining VARCHARs around the excluded
# `password` slot. If the positional decoder mis-handles the excluded
# column (advances the row iterator by the wrong amount when skipping),
# every column after `password` shifts — which would show here as the
# wrong byte counts on display_name / first_name / language.
> SELECT id, char_length(email), octet_length(email), char_length(display_name), octet_length(display_name) FROM accounts ORDER BY id;
1 17 17 5 5
2 80 80 100 100
3 23 55 100 300
4 80 320 100 300
5 15 15 85 255
6 14 14 86 256

#
# DDL #1 — INSTANT ADD COLUMN at the tail (tail = position 24), plus
# UPDATE of an older row whose physical layout is the old 23-col shape.
# The UPDATE's before-image must reconstruct to the logical 24-col shape
# so retractions match insertions.
#
$ mysql-execute name=mysql
ALTER TABLE accounts ADD COLUMN nickname varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, ALGORITHM=INSTANT;
UPDATE accounts SET language = 'es', theme = 'light' WHERE id = 1;
INSERT INTO accounts (email, password, first_name, language, gender, theme, status) VALUES ('grace@example.com', 'pw', 'Grace', 'ko', 'female', 'gray', 'good');
DELETE FROM accounts WHERE id = 6;

> SELECT id, email, language, gender, theme, status FROM accounts ORDER BY id;
1 "alice@example.com" es unknown light good
2 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@example.com" "en-US" female light "in-review"
3 "中中中中中中中中中中中中中中中中@ex.com" "中文" male gray terminated
4 "🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀" zh unknown dark "review-passed"
5 "boundary@ex.com" zh female classic "icr-level-1"
7 "grace@example.com" ko female gray good

#
# DDL #2 — INSTANT ADD multiple columns, including one with a default
# that exercises the "added-column-with-default" storage optimization
# (existing rows don't get physically rewritten but the default still
# appears in binlog row images).
#
$ mysql-execute name=mysql
ALTER TABLE accounts ADD COLUMN login_count int NOT NULL DEFAULT 0, ADD COLUMN tags varchar(200) COLLATE utf8mb3_unicode_ci DEFAULT NULL, ADD COLUMN bio text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, ALGORITHM=INSTANT;
UPDATE accounts SET gender = 'male', mobile_phone = '+1-555-9999' WHERE id = 1;

> SELECT id, email, gender, mobile_phone FROM accounts WHERE id = 1;
1 "alice@example.com" male "+1-555-9999"

#
# DDL #3 — INSTANT DROP COLUMN on an untracked trailing column. The
# dropped column's physical slot remains in older rows' on-disk format
# but is supposed to be stripped from binlog events for new writes.
#
$ mysql-execute name=mysql
ALTER TABLE accounts DROP COLUMN tags, ALGORITHM=INSTANT;
INSERT INTO accounts (email, password, first_name, language, gender, theme, status, bio) VALUES ('henry@example.com', 'pw', 'Henry', 'ja', 'male', 'light', 'good', REPEAT('中', 500));
UPDATE accounts SET status = 'terminated' WHERE id = 3;

# Tracked columns are unaffected by the INSTANT DROP of an untracked tail col.
> SELECT id, email, language, gender, theme, status FROM accounts WHERE id IN (1, 3, 8) ORDER BY id;
1 "alice@example.com" es male light good
3 "中中中中中中中中中中中中中中中中@ex.com" "中文" male gray terminated
8 "henry@example.com" ja male light good

#
# DDL #4 — tighten nullability (not INSTANT-eligible). First fix up any
# NULLs, then do the MODIFY. This exercises schema verification on
# tracked columns without changing the column count.
#
$ mysql-execute name=mysql
UPDATE accounts SET title_id = 0 WHERE title_id IS NULL;
ALTER TABLE accounts MODIFY COLUMN title_id int unsigned NOT NULL DEFAULT 0;
INSERT INTO accounts (email, password, first_name, title_id, language) VALUES ('ivan@example.com', 'pw', 'Ivan', 42, 'ru');

> SELECT id, email, title_id, language FROM accounts WHERE id IN (1, 4, 9) ORDER BY id;
1 "alice@example.com" 0 es
4 "🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀" 0 zh
9 "ivan@example.com" 42 ru

#
# Final sanity — `language` stays ≤ 5 characters across every row
# across every DDL stage, and the ENUMs never decode to something
# outside their declared value sets.
#
> SELECT id, language, char_length(language), octet_length(language), gender, theme, status FROM accounts ORDER BY id;
1 es 2 2 male light good
2 "en-US" 5 5 female light "in-review"
3 "中文" 2 6 male gray terminated
4 zh 2 2 unknown dark "review-passed"
5 zh 2 2 female classic "icr-level-1"
7 ko 2 2 female gray good
8 ja 2 2 male light good
9 ru 2 2 unknown dark good

> DROP SOURCE mz_source CASCADE;
