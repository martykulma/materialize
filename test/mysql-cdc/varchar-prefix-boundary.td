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
# Reproduce a real customer column layout (first 12 columns of an
# `accounts` table) that mixes utf8mb3 and utf8mb4 VARCHARs and straddles
# the 255-byte max-length boundary MySQL uses to decide between 1-byte
# and 2-byte length prefixes in binlog VARSTRING events. Per-column
# byte budget (max bytes = char_count × charset max-bytes-per-char):
#
#   email           VARCHAR(80)  utf8mb4 → 320 bytes → 2-byte prefix
#   alternate_email VARCHAR(80)  utf8mb3 → 240 bytes → 1-byte prefix
#   password        VARCHAR(100) utf8mb3 → 300 bytes → 2-byte prefix
#   first_name      VARCHAR(40)  utf8mb4 → 160 bytes → 1-byte prefix
#   middle_name     VARCHAR(40)  utf8mb4 → 160 bytes → 1-byte prefix
#   last_name       VARCHAR(40)  utf8mb4 → 160 bytes → 1-byte prefix
#   display_name    VARCHAR(100) utf8mb3 → 300 bytes → 2-byte prefix
#   profile_image   VARCHAR(80)  utf8mb3 → 240 bytes → 1-byte prefix
#   language        VARCHAR(5)   utf8mb3 →  15 bytes → 1-byte prefix
#
# The row layout forces the binlog parser to alternate prefix sizes
# within a single row, and the post-snapshot INSERTs probe values at,
# below, and above 255 bytes to exercise every prefix-size code path.
# If the binlog event parser ever miscounts a length prefix (1-byte read
# where 2-byte was written, or vice versa), every column after the
# misread slides — and `language` (a tightly bounded VARCHAR(5)) is the
# first place the resulting garbage becomes visible as a length-check
# failure.
#
# NOTE: testdrive's mysql-execute body treats each line as a separate
# query (no line continuation for raw SQL), so each statement below is
# kept on a single line even when long.
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
# Strict mode off: we are deliberately exercising lengths around the max.
SET SESSION sql_mode = '';
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE accounts (id int unsigned NOT NULL AUTO_INCREMENT, title_id int unsigned DEFAULT NULL, country_id int unsigned NOT NULL DEFAULT '243', email varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, alternate_email varchar(80) COLLATE utf8mb3_unicode_ci DEFAULT NULL, password varchar(100) COLLATE utf8mb3_unicode_ci NOT NULL, first_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, middle_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, last_name varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL, display_name varchar(100) COLLATE utf8mb3_unicode_ci DEFAULT NULL, profile_image varchar(80) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '', language varchar(5) COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT 'en', PRIMARY KEY (id)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb3_unicode_ci;
# Row 1 — pre-snapshot baseline, short ASCII. Exercises small values
# sitting in 2-byte-prefix columns (email, password, display_name).
INSERT INTO accounts (email, password, first_name, last_name, display_name) VALUES ('alice@example.com', 'hunter2', 'Alice', 'Smith', 'alice');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE accounts FROM SOURCE mz_source (REFERENCE public.accounts);

> SELECT id, email, language FROM accounts;
1 "alice@example.com" en

# Row 2 — ASCII at each column's char-count max. Forces the 2-byte-prefix
# columns to emit a non-trivial length while staying under 255 bytes.
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language) VALUES (CONCAT(REPEAT('a', 68), '@example.com'), CONCAT(REPEAT('a', 68), '@example.com'), REPEAT('x', 100), REPEAT('f', 40), REPEAT('m', 40), REPEAT('l', 40), REPEAT('d', 100), REPEAT('p', 80), 'en-US');

# Row 3 — utf8mb3 CJK across the full row. 中 is 3 bytes, so 100 × 中
# produces 300 bytes in password/display_name, exercising the 2-byte
# prefix carrying a value > 255. profile_image lands at 240 bytes (still
# a 1-byte-prefix column, near max).
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language) VALUES (CONCAT(REPEAT('中', 16), '@ex.com'), CONCAT(REPEAT('中', 16), '@ex.com'), REPEAT('中', 100), REPEAT('中', 40), NULL, REPEAT('中', 40), REPEAT('中', 100), REPEAT('中', 80), '中文');

# Row 4 — utf8mb4 4-byte emoji in utf8mb4 columns, utf8mb3 elsewhere.
# email goes to 320 bytes (2-byte prefix, >255). first/middle/last_name
# go to 160 bytes (1-byte prefix, near max).
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language) VALUES (REPEAT('🦀', 80), CONCAT(REPEAT('中', 16), '@ex.com'), REPEAT('中', 100), REPEAT('🦀', 40), REPEAT('🦀', 40), REPEAT('🦀', 40), REPEAT('中', 100), REPEAT('中', 80), 'zh');

# Row 5 — NULLs on every nullable column, empty profile_image, and
# minimum-length language. Exercises the NULL bitmap and zero-length
# prefix reads.
$ mysql-execute name=mysql
INSERT INTO accounts (email, alternate_email, password, first_name, middle_name, last_name, display_name, profile_image, language) VALUES ('empty@ex.com', NULL, 'p', NULL, NULL, NULL, NULL, '', 'a');

# Row 6 — password and display_name at *exactly* 255 bytes (85 × 中).
# The 2-byte prefix carries 0x00FF; a parser that truncates to 1 byte
# would still read 0xFF and the error would only surface past this
# boundary.
$ mysql-execute name=mysql
INSERT INTO accounts (email, password, first_name, display_name, profile_image, language) VALUES ('boundary@ex.com', REPEAT('中', 85), 'Boundary', REPEAT('中', 85), REPEAT('a', 80), 'zh');

# Row 7 — password and display_name at 256 bytes (85 × 中 + 'a').
# The 2-byte prefix now carries 0x0100; the high byte is non-zero, so
# any parser that treats the prefix as 1 byte misreads the row from
# this point on.
$ mysql-execute name=mysql
INSERT INTO accounts (email, password, first_name, display_name, profile_image, language) VALUES ('over255@ex.com', CONCAT(REPEAT('中', 85), 'a'), 'Over', CONCAT(REPEAT('中', 85), 'a'), REPEAT('a', 80), 'en');

# `language` is the tight VARCHAR(5). If any earlier column's length
# prefix was misread, the bytes landing here would exceed 5 characters
# and `check_char_length` would raise a definite error. All seven rows
# must decode cleanly.
> SELECT id, language, char_length(language), octet_length(language) FROM accounts ORDER BY id;
1 en 2 2
2 "en-US" 5 5
3 "中文" 2 6
4 zh 2 2
5 a 1 1
6 zh 2 2
7 en 2 2

# Byte lengths on the 2-byte-prefix columns (password, display_name) and
# on the near-max 1-byte-prefix column (profile_image). Any byte-offset
# drift in the binlog parse would surface as spurious extra or missing
# bytes here, not just on `language`.
> SELECT id, char_length(password), octet_length(password), char_length(display_name), octet_length(display_name), char_length(profile_image), octet_length(profile_image) FROM accounts ORDER BY id;
1 7 7 5 5 0 0
2 100 100 100 100 80 80
3 100 300 100 300 80 240
4 100 300 100 300 80 240
5 1 1 <null> <null> 0 0
6 85 255 85 255 80 80
7 86 256 86 256 80 80

# The utf8mb4 columns should also preserve their exact byte lengths,
# including the emoji case where email crosses 255 bytes.
> SELECT id, char_length(email), octet_length(email), char_length(first_name), octet_length(first_name) FROM accounts ORDER BY id;
1 17 17 5 5
2 80 80 40 40
3 23 55 40 120
4 80 320 40 160
5 12 12 <null> <null>
6 15 15 8 8
7 14 14 4 4

> DROP SOURCE mz_source CASCADE;
