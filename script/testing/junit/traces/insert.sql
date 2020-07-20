# 1 tuple insert, with no column specification.
CREATE TABLE tbl(c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 INTEGER)
INSERT INTO tbl VALUES (1, 2, 3)
SELECT c1, c2, c3 from tbl
DROP TABLE tbl
# 1 tuple insert, with columns inserted in schema order
CREATE TABLE tbl(c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 INTEGER)
INSERT INTO tbl (c1, c2, c3) VALUES (1, 2, 3)
SELECT c1, c2, c3 from tbl
DROP TABLE tbl
# 1 tuple insert, with columns inserted in different order from schema.
# issue #729 wait to be fixed
CREATE TABLE tbl(c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 INTEGER)
INSERT INTO tbl (c3, c1, c2) VALUES (3, 1, 2)
SELECT c1, c2, c3 from tbl
DROP TABLE tbl
# 2 tuple insert, with no column specification
CREATE TABLE tbl(c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 INTEGER)
INSERT INTO tbl VALUES (1, 2, 3), (11, 12, 13)
SELECT c1, c2, c3 from tbl
DROP TABLE tbl
# 2 tuple insert, with no column specification, with fewer than
# schema columns
# binding failed, wait to be fixed
CREATE TABLE tbl(c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 INTEGER)
INSERT INTO tbl VALUES (1), (11, 12)
SELECT c1, c2, c3 from tbl
DROP TABLE tbl
# Test insertion of NULL values.
# Fix #712.
CREATE TABLE xxx (col1 INT)
INSERT INTO xxx VALUES (NULL)
SELECT * FROM xxx
DROP TABLE xxx
# CREATE TABLE with a qualified namespace doesn't work as expected
# #706 fixed but also need #724 for select and drop
CREATE SCHEMA foo
CREATE TABLE foo.bar (id integer)
SELECT * from pg_catalog.pg_class
DROP SCHEMA foo CASCADE
# Invalid Implicit Casting of Integer Strings as Varchars
# #733 fixed
CREATE TABLE xxx01 (col0 VARCHAR(32) PRIMARY KEY, col1 VARCHAR(32))
INSERT INTO xxx01 VALUES ('0', '1319')
INSERT INTO xxx01 VALUES ('1', '21995')
INSERT INTO xxx01 VALUES ('2', '28037')
INSERT INTO xxx01 VALUES ('3', '26984')
INSERT INTO xxx01 VALUES ('4', '2762')
INSERT INTO xxx01 VALUES ('5', '31763')
INSERT INTO xxx01 VALUES ('6', '20359')
INSERT INTO xxx01 VALUES ('7', '26022')
INSERT INTO xxx01 VALUES ('8', '364')
INSERT INTO xxx01 VALUES ('9', '831')
SELECT * FROM xxx01
# Support default value expressions
# #718 fixed
CREATE TABLE xxx (id integer, val integer DEFAULT 123)
INSERT INTO xxx VALUES (1, DEFAULT)
SELECT * from xxx
DROP TABLE xxx
# Support out of order inserts
# #729 fixed
CREATE TABLE xxx (c1 integer, c2 integer, c3 integer)
INSERT INTO xxx (c2, c1, c3) VALUES (2, 3, 4)
SELECT * from xxx
DROP TABLE xxx
# Support out of order inserts with defaults
# #718 fixed
CREATE TABLE xxx (c1 integer, c2 integer, c3 integer DEFAULT 34)
INSERT INTO xxx (c2, c1) VALUES (2, 3)
SELECT * from xxx
DROP TABLE xxx
# Test inserting default expressions that are not INTEGER type.
# #831 BinderSherpa
CREATE TABLE xxx (c1 integer, c2 integer, c3 bigint DEFAULT 4398046511104)
INSERT INTO xxx (c1, c2) VALUES (1, 2)
SELECT * from xxx
DROP TABLE xxx
# Test inserting cast expressions for BIGINT and TIMESTAMP.
# #831 BinderSherpa
CREATE TABLE xxx (c1 bigint, c2 timestamp)
INSERT INTO xxx (c1, c2) VALUES ('123'::bigint, '2020-01-02 12:23:34.56789')
SELECT * from xxx
DROP TABLE xxx