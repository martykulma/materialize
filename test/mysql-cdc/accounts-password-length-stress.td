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
# Stress the excluded `password` column's value length across UPDATE
# events. The customer's accumulated errors on `accounts` are
# consistent with a shift-by-one-column drift starting at position 5
# (the excluded `password`):
#
#   * first_name received 60 bytes       — bcrypt hash length
#   * profile_image received NULL        — display_name's NULL one slot later
#   * language received 36 bytes         — profile_image URL two slots later
#
# Our previous customer-repro test uses a fixed 60-byte bcrypt hash and
# passes. If the drift is triggered by a specific password byte length
# (especially crossing the 1-byte/2-byte length-prefix boundary on a
# VARCHAR(100) utf8mb3 column whose declared max_bytes=300), iterating
# through every interesting length lets us surface it.
#
# The post-snapshot UPDATEs each:
#   1. Write a known password value of specific length/content.
#   2. Simultaneously set first_name / profile_image / language to
#      distinctive, SHORT values we can assert against.
#
# If the decoder drifts for any password length, the fixed values for
# first_name/profile_image/language would be wrong in Materialize. We
# check them after every update.
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
# Pre-snapshot baseline: one row with short-ASCII password.
INSERT INTO accounts (id, country_id, email, password, first_name, last_name, display_name, profile_image, language) VALUES (1000, 91, 'stress@example.com', 'p', 'pre', 'snap', NULL, '', 'en');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE app.accounts) WITH (TEXT COLUMNS (gender, theme, status), EXCLUDE COLUMNS (password));

> SELECT id, email, first_name, profile_image, language FROM accounts;
1000 "stress@example.com" pre "" en

#
# Round 1 — small/empty password values. Anything up to ~15 bytes hits
# the common case of a 1-byte length prefix carrying a small value.
#
$ mysql-execute name=mysql
UPDATE accounts SET password = '', first_name = 'n001', profile_image = 'p001', language = 'en' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n001 p001 en

$ mysql-execute name=mysql
UPDATE accounts SET password = 'a', first_name = 'n002', profile_image = 'p002', language = 'fr' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n002 p002 fr

$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('a', 15), first_name = 'n003', profile_image = 'p003', language = 'de' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n003 p003 de

#
# Round 2 — mid-range ASCII passwords. 32 is typical MD5 hex, 60 is
# bcrypt. 60 is the exact length in the customer's first_name error.
#
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('a', 32), first_name = 'n004', profile_image = 'p004', language = 'es' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n004 p004 es

$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('a', 60), first_name = 'n005', profile_image = 'p005', language = 'it' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n005 p005 it

$ mysql-execute name=mysql
UPDATE accounts SET password = '$2a$08$zmPUPLXPht1yNQaB/cDkvuN7QoREi2jvpKvGr1QZTK0S2Pk0kmPXq', first_name = 'n006', profile_image = 'p006', language = 'pt' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n006 p006 pt

#
# Round 3 — ASCII at the column char limit. 100 ASCII = 100 bytes.
# Under declared max_bytes=300 so still 2-byte prefix.
#
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('x', 100), first_name = 'n007', profile_image = 'p007', language = 'ja' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n007 p007 ja

#
# Round 4 — utf8mb3 values crossing the 1-byte ↔ 2-byte prefix
# boundary on the wire. Even though `password` is declared max_bytes=300
# (so the prefix is always 2 bytes), the VALUE byte count crossing 255
# is the thing that flips the high prefix byte from 0 to non-zero, and
# is the most common corner where length-prefix parsers bug out.
#
# 84 × 中 = 252 bytes, length = 0x00FC → high byte 0
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 84), first_name = 'n008', profile_image = 'p008', language = 'zh' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n008 p008 zh

# 85 × 中 = 255 bytes, length = 0x00FF → high byte 0, still low byte only
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 85), first_name = 'n009', profile_image = 'p009', language = 'ko' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n009 p009 ko

# 86 × 中 = 258 bytes, length = 0x0102 → high byte NONZERO for the
# first time. This is the exact transition that would fail a parser
# that truncated to 1-byte prefix reads.
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 86), first_name = 'n010', profile_image = 'p010', language = 'ru' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n010 p010 ru

# 90 × 中 = 270 bytes, length = 0x010E
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 90), first_name = 'n011', profile_image = 'p011', language = 'uk' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n011 p011 uk

# 100 × 中 = 300 bytes, exactly at max_bytes. length = 0x012C
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 100), first_name = 'n012', profile_image = 'p012', language = 'pl' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n012 p012 pl

#
# Round 5 — mixed-content passwords where specific byte values sit at
# positions that could confuse a parser (e.g. FE / FF sentinel-like
# bytes).
#
# bcrypt hash with '/' and '$' — matches the customer's hash structure
$ mysql-execute name=mysql
UPDATE accounts SET password = '$2b$10$12345678901234567890123456789012345678901234567', first_name = 'n013', profile_image = 'p013', language = 'nl' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n013 p013 nl

# Argon2-style hash with lots of $ signs and = pad
$ mysql-execute name=mysql
UPDATE accounts SET password = '$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$hash', first_name = 'n014', profile_image = 'p014', language = 'sv' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n014 p014 sv

# MD5 hex
$ mysql-execute name=mysql
UPDATE accounts SET password = '5d41402abc4b2a76b9719d911017c592', first_name = 'n015', profile_image = 'p015', language = 'fi' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n015 p015 fi

# SHA-256 hex
$ mysql-execute name=mysql
UPDATE accounts SET password = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', first_name = 'n016', profile_image = 'p016', language = 'no' WHERE id = 1000;

> SELECT first_name, profile_image, language FROM accounts WHERE id = 1000;
n016 p016 no

#
# Round 6 — multiple rows each updated with a different password
# length in quick succession, to exercise the parser across
# transactions with varied-length payloads in the excluded slot.
#
$ mysql-execute name=mysql
INSERT INTO accounts (id, country_id, email, password, first_name, language) VALUES (1001, 91, 'u1@ex.com', '', 'row001', 'en');
INSERT INTO accounts (id, country_id, email, password, first_name, language) VALUES (1002, 91, 'u2@ex.com', REPEAT('a', 60), 'row002', 'en');
INSERT INTO accounts (id, country_id, email, password, first_name, language) VALUES (1003, 91, 'u3@ex.com', REPEAT('中', 85), 'row003', 'en');
INSERT INTO accounts (id, country_id, email, password, first_name, language) VALUES (1004, 91, 'u4@ex.com', REPEAT('中', 86), 'row004', 'en');
INSERT INTO accounts (id, country_id, email, password, first_name, language) VALUES (1005, 91, 'u5@ex.com', REPEAT('中', 100), 'row005', 'en');

> SELECT id, email, first_name, language FROM accounts WHERE id BETWEEN 1001 AND 1005 ORDER BY id;
1001 "u1@ex.com" row001 en
1002 "u2@ex.com" row002 en
1003 "u3@ex.com" row003 en
1004 "u4@ex.com" row004 en
1005 "u5@ex.com" row005 en

#
# Round 7 — multi-row UPDATE that rewrites passwords across the length
# range in a single statement, exercising the case where one binlog
# Update_rows event has many before/after image pairs each with
# different excluded-slot byte counts.
#
$ mysql-execute name=mysql
UPDATE accounts SET password = CASE id WHEN 1001 THEN REPEAT('a', 100) WHEN 1002 THEN REPEAT('中', 86) WHEN 1003 THEN '' WHEN 1004 THEN REPEAT('中', 100) WHEN 1005 THEN REPEAT('a', 60) END, language = CASE id WHEN 1001 THEN 'a1' WHEN 1002 THEN 'a2' WHEN 1003 THEN 'a3' WHEN 1004 THEN 'a4' WHEN 1005 THEN 'a5' END WHERE id BETWEEN 1001 AND 1005;

> SELECT id, first_name, language FROM accounts WHERE id BETWEEN 1001 AND 1005 ORDER BY id;
1001 row001 a1
1002 row002 a2
1003 row003 a3
1004 row004 a4
1005 row005 a5

#
# Round 8 — the final UPDATE sets language to the max 5-char value, so
# if the excluded-slot drift had been writing garbage into language
# all along, this assertion specifically would trip where shorter
# values might have silently passed.
#
$ mysql-execute name=mysql
UPDATE accounts SET password = REPEAT('中', 100), first_name = 'nFIN', profile_image = 'pFIN', language = 'en-US' WHERE id = 1000;

> SELECT first_name, profile_image, language, char_length(language), octet_length(language) FROM accounts WHERE id = 1000;
nFIN pFIN "en-US" 5 5
