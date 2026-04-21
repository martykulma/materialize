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
# Trigger-interleaved reproduction. The customer's binlog shows
# `app.accounts` UPDATE immediately followed by `app.accounts__events`
# INSERT; in the actual binlog these are separate transactions, but
# adding a DB trigger produces them in a single transaction with
# interleaved TABLE_MAP + row events:
#
#     BEGIN
#       Table_map:   app.accounts
#       Update_rows: accounts            ← 23 slots, NULL-heavy bitmap
#       Table_map:   app.accounts__events
#       Write_rows:  accounts__events    ← 10 slots, one TEXT column
#     COMMIT
#
# That's the code path mysql_common exercises when an atomic commit
# touches multiple tables. If there's a bug where parser state from
# one TABLE_MAP leaks into the decode of a subsequent table's row
# event, this is where it'd surface. Our other tests only ever have
# one table per transaction.
#
# accounts__events schema reconstructed from the binlog row shape
# we captured (see mysqlbinlog -vv at GTID 187459184):
#   @1 INT id (PK, auto_increment)
#   @2 INT account_id
#   @3 VARCHAR(45) utf8mb3 — VARSTRING(135) meta=135
#   @4 VARCHAR(45) utf8mb3 — VARSTRING(135) meta=135
#   @5 VARCHAR(200) utf8mb3 — VARSTRING(600) meta=600, nullable
#   @6 TEXT utf8mb3         — BLOB/TEXT meta=2, carries JSON payload
#   @7 VARCHAR(45) utf8mb3 — VARSTRING(135) meta=135, nullable
#   @8 VARCHAR(300) utf8mb3 — VARSTRING(900) meta=900, nullable
#   @9 DATETIME
#   @10 DATETIME
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
CREATE TABLE accounts (id int unsigned NOT NULL AUTO_INCREMENT, title_id int unsigned DEFAULT NULL, country_id int unsigned NOT NULL DEFAULT '243', email varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, alternate_email varchar(80) COLLATE utf8mb3_unicode_ci DEFAULT NULL, password varchar(100) COLLATE utf8mb3_unicode_ci NOT NULL, first_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, middle_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, last_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, display_name varchar(100) COLLATE utf8mb3_unicode_ci DEFAULT NULL, profile_image varchar(80) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '', language varchar(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en', gender enum('male', 'female', 'unknown') COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'unknown', mobile_phone varchar(20) COLLATE utf8mb3_unicode_ci DEFAULT NULL, birthdate date DEFAULT NULL, id_phrase varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, admin int DEFAULT '0', verified int DEFAULT '0', theme enum('light', 'gray', 'dark', 'classic') COLLATE utf8mb3_unicode_ci DEFAULT 'dark', status enum('good', 'in-review', 'terminated', 'icr-level-1', 'icr-level-2', 'review-passed') CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT 'good', created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, deleted datetime DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY email_unique (email)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;
CREATE TABLE accounts__events (id int unsigned NOT NULL AUTO_INCREMENT, account_id int unsigned NOT NULL, event_type varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, event_source varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, description varchar(200) COLLATE utf8mb3_unicode_ci DEFAULT NULL, payload text COLLATE utf8mb3_unicode_ci, ip_address varchar(45) COLLATE utf8mb3_unicode_ci DEFAULT NULL, user_agent varchar(300) COLLATE utf8mb3_unicode_ci DEFAULT NULL, created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id), KEY account_id (account_id)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;

# AFTER UPDATE trigger — fires on every UPDATE to accounts and writes
# an audit row. This is the bit that produces the interleaved
# Table_map + row_event sequence within a single transaction.
$ mysql-execute name=mysql
CREATE TRIGGER accounts_after_update AFTER UPDATE ON accounts FOR EACH ROW INSERT INTO accounts__events (account_id, event_type, event_source, payload, created, modified) VALUES (NEW.id, CASE WHEN OLD.language <> NEW.language THEN 'update_language' WHEN OLD.email <> NEW.email THEN 'update_email' ELSE 'update_other' END, 'update_language_from_user', JSON_OBJECT('to', NEW.id), NOW(), NOW());

# BEFORE-image snapshot, same as accounts-customer-repro.td so the
# UPDATE has the exact NULL bitmap / length-prefix mix on the
# accounts side.
$ mysql-execute name=mysql
INSERT INTO accounts (id, title_id, country_id, email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language, gender, mobile_phone, birthdate, id_phrase, admin, verified, theme, status, created, modified, deleted) VALUES (2060629, NULL, 91, 'oksmar1972@gmail.com', NULL, '$2a$08$zmPUPLXPht1yNQaB/cDkvuN7QoREi2jvpKvGr1QZTK0S2Pk0kmPXq', 'Oksana', NULL, 'Marushchak', NULL, '', 'en', 'female', '+4915739663309', NULL, 'DKEKQ5PT6', 0, 0, 'dark', 'good', '2026-04-17 13:42:46', '2026-04-17 13:42:46', NULL);

# Matches the customer's CREATE SOURCE: password excluded, enums as
# text, and `accounts__events` deliberately NOT tracked — the trigger
# still fires upstream and the audit table's TABLE_MAP + row events
# appear in the binlog stream, but Materialize's source dispatches
# them to no output and discards them. If skipping an un-tracked
# table's events leaves parser state behind that corrupts the next
# tracked table's decode, this is the setup that would catch it.
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE app.accounts) WITH (TEXT COLUMNS (gender, theme, status), EXCLUDE COLUMNS (password));

# Snapshot: only accounts is tracked; accounts__events exists upstream
# but is not a subsource.
> SELECT id, email, language, profile_image FROM accounts;
2060629 "oksmar1972@gmail.com" en ""

#
# UPDATE fires Update_rows for accounts AND (via the trigger) a
# Write_rows for accounts__events within the same binlog transaction.
# Materialize's source reads both Table_map events and both row events
# from the stream; only the accounts one gets dispatched to an output.
# If skipping the un-tracked table's row leaves parser state behind,
# it would corrupt the next tracked decode.
#
$ mysql-execute name=mysql
UPDATE accounts SET language = 'ru', modified = '2026-04-17 13:52:20' WHERE id = 2060629;

> SELECT id, email, language, profile_image FROM accounts;
2060629 "oksmar1972@gmail.com" ru ""

#
# More concurrent-ish updates — several rows flipping language
# and each triggering a fresh audit insert. This stresses the
# multi-row-event-per-transaction path when an UPDATE happens to hit
# multiple rows in one statement.
#
$ mysql-execute name=mysql
INSERT INTO accounts (id, country_id, email, password, first_name, last_name, mobile_phone, id_phrase, created, modified) VALUES (2060630, 91, 'user2@example.com', '$2a$08$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'Anna', 'Baker', '+4915739663310', 'PHRASE002', '2026-04-17 13:42:46', '2026-04-17 13:42:46');
INSERT INTO accounts (id, country_id, email, password, first_name, last_name, mobile_phone, id_phrase, created, modified) VALUES (2060631, 91, 'user3@example.com', '$2a$08$bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'Boris', 'Chen', '+4915739663311', 'PHRASE003', '2026-04-17 13:42:46', '2026-04-17 13:42:46');
UPDATE accounts SET language = 'de' WHERE id IN (2060630, 2060631);

> SELECT id, language FROM accounts ORDER BY id;
2060629 ru
2060630 de
2060631 de

#
# UPDATE that sets a NOT NULL column to empty string (the
# profile_image corner). The trigger still fires upstream, producing
# yet another interleaved binlog sequence.
#
$ mysql-execute name=mysql
UPDATE accounts SET profile_image = '', language = 'es' WHERE id = 2060629;

> SELECT id, language, profile_image FROM accounts WHERE id = 2060629;
2060629 es ""
