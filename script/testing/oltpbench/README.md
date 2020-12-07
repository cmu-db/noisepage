# OLTP Benchmark Testing
This folder include the necessary files for running oltpbench testings.

# OLTP benchmark types
Currently we run the following OLTP benchmarks
- TATP
- TPCC
- NOOP
- YCSB
- Smallbank

## External dependencies
- [oltpbench](https://github.com/oltpbenchmark/oltpbench): for executing the OLTP benchmark of your choice

## Internal dependencies
Let the base directory be `noisepage/script/testing`
- `util/`: provides base classes/functions/constants to construct the `TestOLTPBench` class
  - `test_server.py`
    - Contains the base `TestServer` class to manage the lifecycle of a test suite for a certain OLTP benchmark type
  - `test_case.py`
    - Contains the base `TestCase` class to manage the lifecycle of a test case
  - `common.py`
    - Contains common utility helper functions
  - `constants.py`
    - Contains general constants, like `ErrorCode`
- `reporting/`: provide functions to report testing results to Django API

## File structures
Let the base directory be `noisepage/script/testing/oltpbench`
- `configs/`: contains the JSON config files for Jenkins to run OLTPBench tests for 
  - End-to-end debugging
  - End-to-end performance
  - Nightly

  For more details, please refer to [Configuration file for OLTPBench](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/configs/README.md)
- `run_oltpbench.py`: entry point to run the OLTP bench tests
- `test_oltpbench.py`: defines the `TestOLTPBench` class which manage the lifecycle of a test suite
- `test_case_oltp.py`: defines the `TestOLTPBench` class which manage the lifecycle of a test case
- `utils.py`: defines a list of utility functions specifically used by OLTP bench tests

## Test workflow
- Start the NoisePage DB process as a Python subprocess
  - Try to locate the NoisePage binary
  - Kill all the lingering processes on the NoisePage port
  - Start the NoisePage DB process
- Read the test case configs
- `run_pre_suite`: Pre test suite tasks
  - Clean the possible residual local [oltpbench](https://github.com/oltpbenchmark/oltpbench) workspace
  - Git clone the [oltpbench](https://github.com/oltpbenchmark/oltpbench) repo
  - [optional] *Checkout to the specified branch*
- Iterate through all the test cases
  - `run_pre_test`: Pre test case tasks
    - Create the database and tables for the OLTP benchmark specified
    - Load the data to tables
  - Run the test case command as a subprocess
    - Collect the memory info by using the [PeriodicTask](https://github.com/cmu-db/noisepage/blob/master/script/testing/util/periodic_task.py) in a separate thread
      - Collect `RSS` and `VMS` by default
      - Collect every `5` seconds by default
      - The memory info is stored in a Python dictionary in memory in runtime
  - `run_post_test`: Post test case tasks
    - If it is part of the Jenkins nightly build, the result results should be stored
        - Parse the testing results files by [oltpbench](https://github.com/oltpbenchmark/oltpbench) and format them in JSON
        - Add the memory info to `incremental_metrics` and compute the average metrics to add to the `metrics` in JSON payload
        - Send a POST request to the Django API
- `run_post_suite`: Post test case suite