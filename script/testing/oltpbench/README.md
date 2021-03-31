# OLTP Benchmark Testing
This folder include the necessary files for running oltpbench testings.

## Table of contents
- [OLTP benchmark types](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#oltp-benchmark-types)
- [How to run it?](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#how-to-run-it)
- [Test workflow](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#test-workflow)
- [Dependencies](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#dependencies)
- [File structures](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#file-structures)
- [Config files](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#config-files)

## OLTP benchmark types
Currently we run the following OLTP benchmarks
- TATP
- TPCC
- NOOP
- YCSB
- Smallbank

## How to run it?

### TL;DR
```bash
# This command is sufficient for you to run the OLTP benchmark testing based on your configuration
python3 run_oltpbench.py --config-file=<your-config-file-path>.json
```

### Examples
Currently there are 3 types of the OLTP benchmark testing
- End-to-end debug
  ```bash
  cd noisepage/script/testing/oltpbench
  # To run the TPCC oltpbench test for end-to-end debug on debug version
  python3 run_oltpbench.py \
    --config-file=configs/end_to_end_debug/tpcc.json \
    --build-type=debug
  ```
- End-to-end performance
  ```bash
  cd noisepage/script/testing/oltpbench
  # To run the TPCC oltpbench test for end-to-end performance with wal to be stored in ramdisk on release version
  python3 run_oltpbench.py \
    --config-file=configs/end_to_end_performance/tpcc_wal_ramdisk.json \
    --build-type=release
  ```
- Nightly performance
  ```bash
  cd noisepage/script/testing/oltpbench
  # To run all the oltpbench test case for nightly performance with wal disabled on release version
  python3 run_oltpbench.py \
    --config-file=configs/nightly/nightly_wal_disabled.json \
    --build-type=release
  ```

### OLTPBench test options
For more configurations, there are 2 ways
- Via config files
  - You can refer to the [config files](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#config-files) section below for more details
- Via command line options
comments  - You can use `-h` or `--help` command line option for more details

## Test workflow
Preparation steps of the test
- Start by invoking `run_oltpbench.py`
  - Load the config file passed in and start an instance of the database with any server_args specified in the config
  - Create a test suite for the whole config file and runs the test suite
  - Create a `TestOLTPBench` object for the test execution, since the `TestOLTPBench` is a subclass of the `TestServer` class, it will conduct a list of tasks as the preparation for the actual test, the most important two of which are as follow
    - Pass command line options and configs in to the constructor
    - Try to locate the NoisePage binary
  - Run the test suite by calling `.run()` function of `TestOLTPBench`

The actual test workflow
- Run pre suite tasks: `.run_pre_suite()` function of `TestOLTPBench`
  - Clean the possible residual local [oltpbench](https://github.com/oltpbenchmark/oltpbench) workspace
  - Download and install the [oltpbench](https://github.com/oltpbenchmark/oltpbench) from GitHub
- Iterate through all the test cases
  - (Re)start the NoisePage DB process as a Python subprocess
    - Start the DB if its not running
      - *Kill all the lingering processes on the NoisePage port before starting the NoisePage DB process*
    - Restart it if `db_restart` was passed into the test_case config
    - Otherwise, leave it running
  - Run pre test case tasks: `.run_pre_test()` function of `TestCaseOLTPBench`
    - Create the database and tables for the OLTP benchmark specified
    - Load the data to tables
  - Run the test case command as a subprocess
    - Collect the memory info by using the [PeriodicTask](https://github.com/cmu-db/noisepage/blob/master/script/testing/util/periodic_task.py) in a separate thread
      - Collect `RSS` and `VMS` by default
      - Collect every `5` seconds by default
      - The memory info is stored in a Python dictionary in memory in runtime
  - Run post test case tasks: `.run_post_test()` function of `TestCaseOLTPBench`
    - If `publish-results`, `publish-username`, and `publish-password` arguments are supplied, the result results should be stored
        - Parse the testing results files by [oltpbench](https://github.com/oltpbenchmark/oltpbench) and format them in JSON
        - Add the memory info to `incremental_metrics` and compute the average metrics to add to the `metrics` in JSON payload
        - Send a POST request to the Django API
- Run post suite tasks: `.run_post_suite()` function of `TestOLTPBench`

## Dependencies

### External dependencies
- [oltpbench](https://github.com/oltpbenchmark/oltpbench): for executing the OLTP benchmark of your choice
- [NoisePage Stats](https://github.com/cmu-db/noisepage-stats): for the result reporting API and storage

### Internal dependencies
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

  For more details, check the [config files](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench#config-files) section below for more details
- `run_oltpbench.py`: entry point to run the OLTP bench tests
- `test_oltpbench.py`: defines the `TestOLTPBench` class which manage the lifecycle of a test suite
- `test_case_oltp.py`: defines the `TestOLTPBench` class which manage the lifecycle of a test case
- `utils.py`: defines a list of utility functions specifically used by OLTP bench tests

## Config files
A config file is necessary to specify a test for OLTP Benchmark.

### Fields
In the configuration file, those information are required:
- A list of test cases in `testcases`
- The benchmarks and options in each test case, required by the oltpbench's workload descriptor file
  - The `run_oltpbench` script will run all test cases in the configuration file sequentially. 
  - The `loop` key in the configuration file is used to duplicate the test case with different options.
  - The `server_args` object in the configuration specifies the command line arguments passed to the server when running the binary.

### Example:
The following example shows a oltpbenchmark test with 4 testcases
- The first testcase has the base testcase of terminal `1` but it will be looped for different configurations of the terminals from `1` to `2` to `4`
- The second testcase only has the base testcase whose terminal configuration is `8`

```json
{
    "type": "oltpbenchmark",
    "server_args":{
        "connection_thread_count": 32,
        "wal_file_path": "/mnt/ramdisk/noisepage-wal.log"    
    },
    "testcases": [
        {
            "base": {
                "benchmark": "tatp",
                "weights": "2,35,10,35,2,14,2",
                "query_mode": "extended",
                "scale_factor": 1,
                "terminals": 1,
                "loader_threads": 4,
                "client_time": 60
            },
            "loop": [
                {"terminals":1, "db_create":true,"db_load":true},
                {"terminals":2, "db_restart":false,"db_create":false,"db_load":false},
                {"terminals":4, "db_restart":false,"db_create":false,"db_load":false},
            ]
        },
        {
            "base": {
                "benchmark": "tatp",
                "weights": "2,35,10,35,2,14,2",
                "query_mode": "extended",
                "terminals": 8,
                "loader_threads": 4,
                "client_time": 600
            }
        }
    ]
}
```
