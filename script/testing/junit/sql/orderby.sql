CREATE TABLE date1 (c1 INTEGER NOT NULL PRIMARY KEY, c2 INTEGER, c3 DATE);

INSERT INTO date1 VALUES (1, 2, '1993-07-01');
INSERT INTO date1 VALUES (2, 3, '1993-07-01');
INSERT INTO date1 VALUES (3, 5, '1993-07-02');
INSERT INTO date1 VALUES (4, 1, '1993-07-02');

SELECT c2 from date1 order by c2;
SELECT c2 as alias_c from date1 order by alias_c;
SELECT c2 as alias_c, c3 from date1 order by alias_c;
SELECT c2 as alias_c, c3 from date1 order by 1;
SELECT c2, c3 as alias_c from date1 order by 1;
SELECT c2, c3 as alias_c from date1 order by 2, 1;
SELECT c2, c3 as alias_c from date1 order by alias_c, c2;
SELECT sum(c2) as alias_c from date1 group by c3 order by alias_c;
SELECT sum(c2) from date1 group by c3 order by 1;
SELECT sum(c2), min(c2) as c2_min from date1 group by c3 order by c2_min;
SELECT sum(c2), min(c2), max(c2) from date1 group by c3 order by 3;
SELECT sum(c2+1), min(c2-1), max(c2*c2) from date1 group by c3 order by 3;
SELECT sum(c2+1) AS s, min(c2-1) AS m, max(c2*c2) AS x from date1 group by c3 order by 3 DESC;

DROP TABLE date1;
