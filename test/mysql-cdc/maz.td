# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# ENUM supported only as TEXT
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
SET SESSION sql_mode='';
SET GLOBAL binlog_row_metadata = 'FULL';
USE public;
CREATE TABLE foo (a int, b enum('b1', 'b2'), c enum('c1', 'c2'));
INSERT INTO foo VALUES (0, NULL, NULL), (1, 'b1', 'c1'), (2, 'b2', 'c2');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE foo FROM SOURCE mz_source (REFERENCE public.foo) WITH (TEXT COLUMNS (b, c));

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s

$ mysql-execute name=mysql
INSERT INTO foo VALUES (3, 'b1', 'c1');
