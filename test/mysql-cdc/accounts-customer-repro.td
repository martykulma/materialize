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
# Reproduces the exact row shape and operation at customer GTID
# d5b8f8dc-839c-11ed-a2a9-02b055d74903:187459183 — an UPDATE on
# `app.accounts` that flipped `language` from 'en' to 'ru' for user
# oksmar1972@gmail.com (id=2060629), simultaneously bumping the
# `modified` DATETIME. The customer's `accounts` subsource has been
# accumulating definite errors clustered near this GTID on v26.19.0
# against RDS MySQL 8.0.44 with binlog_row_metadata=minimal.
#
# Server-side emission of this event (verified via mysqlbinlog -vv) is
# clean: 23 slots, correct metas (VARSTRING(320)/240/300/160/240/15,
# ENUM meta=63233, DATETIME meta=0), `profile_image` (@11) is an empty
# string with is_null=0, `language` (@12) is 2 bytes, no value exceeds
# its declared length. Under FULL metadata in our test harness this
# decodes without issue. Running it under MINIMAL metadata (via
# workflow_minimal_metadata) against the v26.19.0 mysql_async/
# mysql_common versions is what's diagnostic.
#
# Specifically reproduces the NULL bitmap pattern from the binlog:
# @2 (title_id), @5 (alternate_email), @8 (middle_name),
# @10 (display_name), @15 (birthdate), @23 (deleted) are NULL, and
# @11 (profile_image) is empty-string-but-NOT-NULL — the exact
# combination that's on the wire.
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
DROP DATABASE IF EXISTS app;
CREATE DATABASE app;
USE app;
CREATE TABLE accounts (id int unsigned NOT NULL AUTO_INCREMENT, title_id int unsigned DEFAULT NULL, country_id int unsigned NOT NULL DEFAULT '243', email varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, alternate_email varchar(80) COLLATE utf8mb3_unicode_ci DEFAULT NULL, password varchar(100) COLLATE utf8mb3_unicode_ci NOT NULL, first_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, middle_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, last_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, display_name varchar(100) COLLATE utf8mb3_unicode_ci DEFAULT NULL, profile_image varchar(80) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '', language varchar(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en', gender enum('male', 'female', 'unknown') COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'unknown', mobile_phone varchar(20) COLLATE utf8mb3_unicode_ci DEFAULT NULL, birthdate date DEFAULT NULL, id_phrase varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, admin int DEFAULT '0', verified int DEFAULT '0', theme enum('light', 'gray', 'dark', 'classic') COLLATE utf8mb3_unicode_ci DEFAULT 'dark', status enum('good', 'in-review', 'terminated', 'icr-level-1', 'icr-level-2', 'review-passed') CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT 'good', created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, deleted datetime DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY email_unique (email), KEY country_id (country_id), KEY email__password (email, password), KEY id_phrase (id_phrase), KEY accounts_created_idx (created), KEY title_id (title_id), KEY acc_id_deleted (id, deleted, admin), KEY acc_admin (admin)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;
# BEFORE-image snapshot: the exact pre-UPDATE state of user
# oksmar1972@gmail.com as captured in the binlog WHERE clause.
# NULLs on title_id, alternate_email, middle_name, display_name,
# birthdate, deleted; empty string on profile_image (NOT NULL).
INSERT INTO accounts (id, title_id, country_id, email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language, gender, mobile_phone, birthdate, id_phrase, admin, verified, theme, status, created, modified, deleted) VALUES (2060629, NULL, 91, 'oksmar1972@gmail.com', NULL, '$2a$08$zmPUPLXPht1yNQaB/cDkvuN7QoREi2jvpKvGr1QZTK0S2Pk0kmPXq', 'Oksana', NULL, 'Marushchak', NULL, '', 'en', 'female', '+4915739663309', NULL, 'DKEKQ5PT6', 0, 0, 'dark', 'good', '2026-04-17 13:42:46', '2026-04-17 13:42:46', NULL);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE app.accounts) WITH (TEXT COLUMNS (gender, theme, status), EXCLUDE COLUMNS (password));

# Snapshot should land the row intact. profile_image is empty-string
# not NULL; language is 2 bytes; everything else matches the customer
# row exactly.
> SELECT id, email, language, profile_image, gender, theme, status FROM accounts;
2060629 "oksmar1972@gmail.com" en "" female dark good

> SELECT char_length(language), octet_length(language), char_length(profile_image), octet_length(profile_image) FROM accounts;
2 2 0 0

#
# The UPDATE from GTID 187459183: language 'en' → 'ru', modified
# bumped to reflect the write. Under RBR this emits a single
# Update_rows_v1 with 23 slots in the BEFORE image and 23 in the
# AFTER image. Every NULL-bitmap bit and every length prefix that
# mysql_common has to parse matches the customer's failing event.
#
$ mysql-execute name=mysql
UPDATE accounts SET language = 'ru', modified = '2026-04-17 13:52:20' WHERE id = 2060629;

# If mysql_common's binlog parser drifts on this shape under MINIMAL
# row metadata (the hypothesis), the decoder reaches @11 or @12 with
# either a stale NULL bit or mis-aligned value bytes, and one of:
#   - `received string value of length N for column language`
#   - `received a null value in a non-null column` on profile_image
# — the two error shapes the customer has seen — would fire on this
# SELECT. Clean decode means the hypothesis doesn't hold for this
# specific event and we need to keep digging.
> SELECT id, email, language, profile_image, gender, theme, status FROM accounts;
2060629 "oksmar1972@gmail.com" ru "" female dark good

> SELECT char_length(language), octet_length(language), char_length(profile_image), octet_length(profile_image) FROM accounts;
2 2 0 0

#
# Same update shape but run several more times against copies of the
# row at different ids, to exercise the parser on repeated events with
# the same NULL/length combination — the customer has been producing
# one error per upstream write event.
#
$ mysql-execute name=mysql
INSERT INTO accounts (id, country_id, email, password, first_name, last_name, mobile_phone, id_phrase, admin, verified, created, modified) VALUES (2060630, 91, 'user2@example.com', '$2a$08$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'Anna', 'Baker', '+4915739663310', 'PHRASE002', 0, 0, '2026-04-17 13:42:46', '2026-04-17 13:42:46');
INSERT INTO accounts (id, country_id, email, password, first_name, last_name, mobile_phone, id_phrase, admin, verified, created, modified) VALUES (2060631, 91, 'user3@example.com', '$2a$08$bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'Boris', 'Chen', '+4915739663311', 'PHRASE003', 0, 0, '2026-04-17 13:42:46', '2026-04-17 13:42:46');
UPDATE accounts SET language = 'de' WHERE id = 2060630;
UPDATE accounts SET language = 'es' WHERE id = 2060631;
UPDATE accounts SET language = 'fr' WHERE id = 2060629;

> SELECT id, email, language, profile_image, gender FROM accounts ORDER BY id;
2060629 "oksmar1972@gmail.com" fr "" female
2060630 "user2@example.com" de "" unknown
2060631 "user3@example.com" es "" unknown
