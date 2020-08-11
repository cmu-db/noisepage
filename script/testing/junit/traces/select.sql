# SELECT With Duplicate Columns Produces Zero Results
# #720 fixed
CREATE TABLE tbl(c1 int NOT NULL PRIMARY KEY, c2 int, c3 int)
INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4)
SELECT c1,c1 FROM tbl
DROP TABLE tbl
# SELECT With Alias
# #795 fixed
CREATE TABLE tbl(c1 int NOT NULL PRIMARY KEY, c2 int, c3 int)
INSERT INTO tbl VALUES (1, 2, 3), (2, 3, 4)
SELECT c1 as new_name FROM tbl
SELECT 1 as new_name FROM tbl
SELECT c1 as c2, c2 as c1 FROM tbl
SELECT COUNT(*) AS cnt FROM tbl HAVING COUNT(*) > 10
SELECT c1 as c1, c2 as c2 FROM tbl
SELECT c1, c2 as c1 FROM tbl
INSERT INTO tbl VALUES (3, 2, 10), (4, 3, 20)
SELECT COUNT(DISTINCT c2) AS STOCK_COUNT FROM tbl
DROP TABLE tbl
# Test for selecting with a timestamp in the where clause.
# Fix #782: selecting with timestamp in where clause will crash the system.
CREATE TABLE xxx (c1 int, c2 timestamp)
INSERT INTO xxx (c1, c2) VALUES (1, '2020-01-02 12:23:34.56789')
INSERT INTO xxx (c1, c2) VALUES (2, '2020-01-02 11:22:33.721-05')
SELECT * FROM xxx WHERE c2 = '2020-01-02 11:22:33.721-05'
DROP TABLE xxx
# SELECT with arithmetic on integer and reals
CREATE TABLE tbl (a int, b float)
INSERT INTO tbl VALUES (1, 1.37)
SELECT a + b AS AB FROM tbl
SELECT 2+3
DROP TABLE tbl
# self-join
CREATE TABLE xxx (id INT PRIMARY KEY, val INT);
INSERT INTO xxx VALUES (1,1),(2,2),(3,3),(4,4);
SELECT * FROM xxx AS x1 JOIN xxx AS x2 ON x1.id = x2.id;
DROP TABLE xxx