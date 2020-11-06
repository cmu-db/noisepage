-- Generate tracefile with:
--     ant generate-trace -Dpath=sql/update.sql -Ddb-url=jdbc:postgresql://localhost/postgres -Ddb-user=postgres -Ddb-password="postgres" -Doutput-name=update.test
CREATE TABLE update1 (c1 int, c2 timestamp);
INSERT INTO update1 (c1, c2) VALUES (1, '2020-01-02 12:23:34.567893');
INSERT INTO update1 (c1, c2) VALUES (2, '2020-01-02 11:22:33.721052');
UPDATE update1 SET c2 = '2020-01-03 11:22:33.721058' WHERE c1 = 2;
SELECT * from update1;
DROP TABLE update1;


CREATE TABLE update2 (c1 int, c2 INTEGER);
INSERT INTO update2 (c1, c2) VALUES (1, 1);
INSERT INTO update2 (c1, c2) VALUES (22, 22);
INSERT INTO update2 (c1, c2) VALUES (23, 33);
UPDATE update2 SET c2 = 4 WHERE c1 = 22;
SELECT * FROM update2;
SELECT * FROM update2 WHERE c2=22;
SELECT c2 FROM update2 WHERE c1=23;
DROP TABLE update2;


CREATE TABLE update3 (c1 int, c2 float);
INSERT INTO update3 (c1, c2) VALUES (1, 1.0);
INSERT INTO update3 (c1, c2) VALUES (2, 2.0);
INSERT INTO update3 (c1, c2) VALUES (3, 3.0);
UPDATE update3 SET c2 = 4.0 WHERE c1 = 2;
SELECT * FROM update3;
SELECT * FROM update3 WHERE c2=2.0;
SELECT c2 FROM update3 WHERE c1=2;
UPDATE update3 SET c1=2 WHERE c2=1.0;
SELECT * FROM update3 WHERE c1=2;
DROP TABLE update3;


CREATE TABLE update4 (c1 int, c2 float, c3 varchar);
INSERT INTO update4 (c1, c2, c3) VALUES (1, 1.0, '1');
INSERT INTO update4 (c1, c2, c3) VALUES (3, 3.0, '3');
UPDATE update4 SET c2 = 4.0 WHERE c1 = 2;
SELECT * FROM update4;
SELECT * FROM update4 WHERE c2=2.0;
SELECT * FROM update4 WHERE c2=2.0 AND c3='2';
SELECT c1 FROM update4 WHERE c2=2.0 OR c3='1';
SELECT * FROM update4 WHERE c2=2.0 AND c3='2' OR c1=1;
SELECT c2 FROM update4 WHERE c1=2;
UPDATE update4 SET c1=2 WHERE c2=1.0;
SELECT * FROM update4 WHERE c1=2;
DROP TABLE update4;

CREATE TABLE update5 (a int)
INSERT INTO update5 (a) VALUES (1),(2),(3);
UPDATE update5 SET a=a;
SELECT a FROM update5;
DROP TABLE update5
