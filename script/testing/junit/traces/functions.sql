# Initialize the database and table for testing
CREATE TABLE data (int_val INT, double_val DECIMAL, str_i_val VARCHAR(32), str_a_val VARCHAR(32), is_null INT)
INSERT INTO data(int_val, double_val, str_i_val, str_a_val, is_null) VALUES(123, 12.34, '123456', 'AbCdEf', 0)
INSERT INTO data(int_val, double_val, str_i_val, str_a_val, is_null) VALUES(NULL, NULL, NULL, NULL, 1)
# Tests usage of trig udf functions
# #744 test
SELECT cos(double_val) AS result FROM data WHERE is_null = 0
SELECT cos(double_val) AS result FROM data WHERE is_null = 1
SELECT sin(double_val) AS result FROM data WHERE is_null = 0
SELECT sin(double_val) AS result FROM data WHERE is_null = 1
SELECT tan(double_val) AS result FROM data WHERE is_null = 0
SELECT tan(double_val) AS result FROM data WHERE is_null = 1
SELECT lower(str_a_val) AS result FROM data WHERE is_null = 0
SELECT lower(str_a_val) AS result FROM data WHERE is_null = 1
DROP TABLE data