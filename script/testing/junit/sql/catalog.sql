# NOTE: This source SQL file may fail with some versions of Postgres

CREATE TABLE t (a VARCHAR);
SELECT atttypmod FROM pg_attribute WHERE attrelid IN (SELECT reloid FROM pg_class WHERE relname = 't');
DROP TABLE t;

CREATE TABLE t (a VARCHAR(55));
SELECT atttypmod FROM pg_attribute WHERE attrelid IN (SELECT reloid FROM pg_class WHERE relname = 't');
DROP TABLE t;
