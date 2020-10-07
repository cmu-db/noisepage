# JUnit Testing Script

This directory contains tests using Junit5.

### src directory

1. The moglib directory contains modified APIs from Wan's mogjdbc library 
used to handle database interaction

2. GenerateTrace converts input file consisting of sql statements to trace file format,
generate exact result if the number of result is less than Constants.DISPLAY_RESULT_SIZE

3. There are two sets of tests: trace tests and non-trace related junit tests.

* TracefileTest takes in path to a trace file from an environment variable called 
NOISEPAGE_TRACE_FILE and dynamically generate a test case for each query. 
(Test case: execute the query, get the result from database and
check if the hash/result/len match). If a non-select query failed and the error
code is provided in the trace, the codes will be compared as well.

* TrafficCopTest and WireTest are non-trace related junit tests.

4. TestUtility provides a list of utility methods. It contains supporting functions 
shared across tests. Supplement as needed. Avoid duplicating code in tests.

### traces directory

The traces directory contains trace test files that end with .test

### Instruction to use FilterTrace:

1. Establish a local postgresql database

2. Start the database server with "pg_ctl -D /usr/local/var/postgres start"

3. Prepare your input trace file

4. In the command line, after the code compiles (ant compile), run 
ant filter-trace with 6 arguments: path, db-url, db-user, db-password, skip-list, and output name
Command format: ant filter-trace -Dpath=PATH_TO_YOUR_FILE
 -Ddb-url=YOUR_JDBC_URL -Ddb-user=YOUR_DB_USERNAME -Ddb-password=YOUR_DB_PASSWORD
 -Dskip-list=DESIRED_SKIP_LIST -Doutput-name=OUTPUT_FILE_NAME

5. An output trace file should be produced called xxx_new (XXX is the input)
Rename output.txt to contain .test (like xxx.test) for it to be included in 
future junit test runs

Example: 

Given sql input file select.test under traces directory, jdbc url jdbc:postgresql://localhost/jeffdb,
db username jeffniu, password "", skip-list [CASE,BETWEEN,WHERE,abs(,WHEN], and output name select_new.test,
the command should be:

ant filter-trace -Dpath=traces/select.test -Ddb-url=jdbc:postgresql://localhost/jeffdb 
-Ddb-user=jeffniu -Dpassword="" -Dskip-list=“CASE,BETWEEN,WHERE,abs(,WHEN" -Doutput-name=select_new.test

The output file contains all traces as before, except that for a query that contain
any keyword within the skip-list, a "skip" tag is added above the query so that when
TracefileTest is run the query would be skipped. For a query that doesn't contain
any keyword within the skip-list, its hash will be updated with postgresql.

### Instruction to use GenerateTrace:

1. Establish a local postgresql database

2. Start the database server with "pg_ctl -D /usr/local/var/postgres start"

3. Write your own sql input file. Format: sql statements, one per line, 
comments allowed (start line with #)

4. In the command line, after the code compiles (ant compile), run 
ant generate-trace with 5 arguments: path, db-url, db-user, db-password and output name
Command format: ant generate-trace -Dpath=PATH_TO_YOUR_FILE -Ddb-url=YOUR_JDBC_URL 
-Ddb-user=YOUR_DB_USERNAME -Ddb-password=YOUR_DB_PASSWORD -Doutput-name=OUTPUT_FILE_NAME

5. An output file should be produced which is of trace file format.
If it contains .test (like xxx.test), then it will be included in 
future junit test runs

Example: 

Given sql input file select.sql under traces directory, jdbc url jdbc:postgresql://localhost/jeffdb,
db username jeffniu, password "", and output name select_new.test, the command should be:

ant generate-trace -Dpath=traces/select.sql -Ddb-url=jdbc:postgresql://localhost/jeffdb
 -Ddb-user=jeffniu -Ddb-password="" -Doutput-name=select_new.test
 
Comments are supported for input files (line starting with "#")
Inlucde "Fail" in comment if you expect the query to fail
Use the format "Num of outputs" to specify the expected number of outputs; if a number is specified,
then in TracefileTest the queryresult length are checked as well.
Use the format "Include Outputs" to specify that you want to store the exact query result instead
of hash

Example input line for GenerateTrace: 
# Num of outputs: 2
SELECT t1 FROM TableA

Then in TracefileTest, the sql statement will be executed and in addition to checking if the hash
match, the number of results will be checked as well. An exception will be thrown if we don't
get 2 results for the example.

Example input line for GenerateTrace: 
# Include Outputs
SELECT t1 FROM TableA

Then, in the tracefile, result from the query, let's say, [1,2,3], will be stored instead of the hash


## Installation and pre-requisites

You will need the following packages:
* java JDK or JRE
* ant

The necessary Java libraries supporting these tests are included in the `lib` directory.

### Installing ant

On Ubuntu:
`sudo apt-get install ant`

### Running the tests

The tests are integrated into Travis and Jenkins. They may also be run manually.
There are several different ways in which the tests may be run.

### Help
For a description of arguments to the program

`./run_junit.py --help`


## Quick Start

Run using the python script. This will compile the tests, run the database server (provided it has 
been built in this development tree), and run the tests.

`./run_junit.py`

run_junit.py will execute the non-trace related junit tests first and then the trace tests.

## Manual Run with the JUnit Console Runner
This method provides clear, human friendly output. This requires Java 8 or later.

1. Compile the tests.
   `ant compile`

2. Start the database server manually in a separate shell (e.g., `./terrier`)

3. To run a trace test, pass in file path as an environment variable.
Example command: NOISEPAGE_TRACE_FILE=./traces/select.test ant test-trace

4. To run non-trace tests, run ant test-unit

## Adding New Tests

No changes are necessary to the antfile (build.xml). 
To add trace tests, add the trace files ending with .test (`XXX.test`) to the junit/traces directory.
To add non-trace junit tests, add the java test files (`XXX.java`) to the junit/src directory.
Ant compiles all java files present in the `src` directory. The test runners find all compiled tests and run them.

## Code Structure for Tests

* See `TrafficCopTest.java` for an example of a non-trace junit test. Each test will need a `Connection`
  variable and SQL statements to setup the tables.

* JUnit invokes the `Setup()` method prior to each test and then calls `Teardown()` after they
  finish. If you need different granularity, see the JUnit5 documentation. Class
  level setup / teardown is possible.

* Functions annotated with `@Test` are the actual tests. 
  Tests may be temporarily disabled by commented out the annotation (e.g., `//@Test`).
