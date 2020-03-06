# JUnit Testing Script

This directory contains tests using Junit4 and JDBC.

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


## Manual Run with Ant JUnit Runner

1. Compile the tests.
   `ant compile`

2. To run the tests, first start the database server manually in a separate shell (e.g., `./terrier`)

3. Run the tests, (from this directory):
   `ant test`

## Manual Run with the JUnit Console Runner
This method provides clearer, human friendly output. This requires Java 8 or later.

1. Compile the tests.
   `ant compile`

2. To run the tests, first start the database server manually in a separate shell (e.g., `./terrier`)

3. Run the tests, (from this directory):
   `ant testconsole`

## Adding New Tests

No changes are necessary to the antfile (build.xml). Ant compiles all java files present in 
the `src` directory. The test runners find all compiled tests and run them.

## Code Structure for Tests

* `TestUtility` contains supporting functions shared across tests. Supplement as needed. Avoid duplicating code in tests.

* Add a new `XXX.java` file for each new test class. Each class may contain as many tests as desired. The current tests use one schema for the class.

* See `UpdateTest.java` for an example of a simple test. Each test will need a `Connection`
  variable and SQL statements to setup the tables.

* See InsertPSTest.java for additional examples, including use of prepared statements. 
  Most likely you'll want to implement using normal statements rather than prepared statements.

* JUnit invokes the `Setup()` method prior to each test and then calls `Teardown()` after they
  finish. If you need different granularity, see the JUnit4 documentation. Class
  level setup / teardown is possible.

* Functions annotated with `@Test` are the actual tests. 
  Tests may be temporarily disabled by commented out the annotation (e.g., `//@Test`).


