# Testing Scripts

## Documentation

Read and follow this: https://numpydoc.readthedocs.io/en/latest/format.html

## Folder structure
All tests are compatible with python3
- `util`: all the common utilities for running all kinds of tests
- `junit`: entry script to fire a junit test (and many other supporting configs)
- `micro_bench`: entry script to run the microbenchmark tests
- `oltpbench`: entry script to fire an oltp bench test
- `artifact_stats`: entry script to collect the artifact stats
- `reporting`: utility scripts for posting test data to Django API and formating JSON payloads

## Util
`util` folder contains a list of common Python scripts
- `common.py`: functions that can be used in many different settings
- `constants.py`: all the constants used in the any file under the `util` or across the different tests
- `db_server.py`: It provides a `NoisePageServer` class that can start, stop, or restart an instance of the NoisePage
- `test_server.py`: the base `TestServer` class for running all types of tests
- `test_case.py`: the base `TestCase` class for all types of test cases.
- `mem_metrics.py`: the `MemoryMetric` class and `MemoryInfo` named tuple to manage the memory related information during the run time of the tests.
- `periodic_task.py`: the `PeriodicTask` class provides a general utility in Python which runs a separate thread that will execute a subprocess every `x` seconds until told to stop.

## OLTP Bench
`oltpbench` folder contains Python scripts for running an oltp bench test. Refer to [OLTP Benchmark Testing](https://github.com/cmu-db/noisepage/tree/master/script/testing/oltpbench/README.md) for more details.

## How to run a test
To run a test of a certain type, just run the `run_<TEST TYPE>.py` script in the respective folder. For example, if you want to run a junit test, just simply run `python3 junit/run_junit.py`.

By doing that, `junit/run_junit.py` script will try to import the `TestJUnit` class from the `util/TestJunit.py`, which subsequently use most of the functionalities provided from its super class `TestServer` from `util/TestServer.py`.

## QueryMode
For both `junit` and `oltpbench`, we support 2 query modes with the optional argument `--query-mode`
- `simple` (default if not specified)
- `extended`

If you specify the `--query-mode extended`, you then can also indicate the prepare threshold (default is `5`) with the optional argument `--prepare-threshold` with type `int`. Please be reminded that if you choose the query mode as `simple`, the prepare threshold will be ignored.

## TestServer
`TestServer` is the base class for running all types of the tests. 

### Test workflow
- check if the noisepage bin exists
- run the pre-suite task (test suite specific)
  - e.g. install oltp bin 
- run the test sequentially
  - [Optional] fork a subprocess to start the DB (via python subprocess.Popen) 
    - if skip this step, the test will run on the used database
  - run the pre-test task (test specific)
  - fork a subprocess to start the test process using the command (via python subprocess.Popen)
  - check the return code from the OS
  - write the stdout and the stderr to the test output log file
  - run the post-test task (test specific)
  - [Optional] stop the DB
    - if skip this step, the populated database can be used for following experiments
- run the post-suite task (test suite specific) 
- print out the logs to the stdout

### Adding a new test case
The classes in the `util` folder can be used and extend to help you create a new test type.

All test cases should inherit from the `TestCase` class. Anyone is free to modify any attribute from the base class.
- Mandatory attributes
  - `test_command` (`List(str)`): the command to run the test case
- Optional attributes
  - `test_command_cwd` (`str`): the working directory to run the test command
- Optional functions
  - `run_pre_test`: the pre-test tasks required for the test
    - config the xml file, etc.
  - `run_post_test`: the post-test tasks required for the test
    - e.g. parse the output json, etc.

### Base classes
- `NoisePageServer`
  - Manage the lifecycle of the NoisePage instance. It create a Python subprocess for the NoisePage process, poll the logs, and terminate or kill it when the test finishes
- `TestCase`
  - Manage a the life cycle of a test case. A test case is usually a process trigger from command line. The actual test case can be specified as a command string and created and executed in a Python subprocess. 
  - The `TestCase` class also provides `run_pre_test` and `run_post_test` functions for you to override for preparation and clean up of each test case.
- `TestServer`
  - Manage the entire lifecycle of a test. It uses the `NoisePageServer` to manage the database process. One `TestServer` can have a list of `TestCase`s and treats the entire collection as a suite. 
  - It also provides the `run_pre_suite` and `run_post_suite` functions for you to override to specify any preparation and cleanup at the suite level.

### Step-by-step instructions
- Create the folder for your test under `noisepage/script/testing/<mytest>`
- In the folder of your test, create the following files
  - `run_<mytest>.py`
    - The entry script for your test, which specifies the command line arguments and options.
    - You can refer to [oltpbench/run_oltpbench.py](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/run_oltpbench.py) for reference.
  - `test_<mytest>.py`
    - The main test class for your test, which should be a subclass of the `TestServer` in `util/test_server.py`.
    - You can refer to [oltpbench/test_oltpbench.py](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/test_oltpbench.py) for reference.
  - [optional] `test_case_<mytest>.py`
    - The base test case class for your test, which should be a subclass of the `TestCaseServer` in `util/test_case_server.py`.
    - You can refer to [oltpbench/test_case_oltp.py](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/test_case_oltp.py) for reference.
  - [optional] `constants.py`
    - Contains all the constants the test need to use. Usually you should add the commands you need to run for your test and pre/post_suite tasks and as pre/post_test tasks constants here. It can be imported and used in your `test_<mytest>.py` or `test_case_<mytest>.py`.
    - You can refer to [oltpbench/constants.py](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/constants.py) for reference.
  - [optional] `util.py`
    - Contains all the utility functions the test need to use.
    - You can refer to [oltpbench/util.py](https://github.com/cmu-db/noisepage/blob/master/script/testing/oltpbench/util.py) for reference.
- Create a stage for your test in Jenkins pipeline
  - Go to `noisepage/Jenkinsfile`, create a stage at the place of your choice, and create the stage based on the template config as below.
  ```groovy
  stage('My Test') {
      parallel{
          stage('My Test Name 1') {
              agent { label 'macos' }
              environment {
                  <!-- Add environment variables here -->
              }
              steps {
                  <!-- Add a list of shell commands here -->
              }
              post {
                  cleanup {
                      deleteDir()
                  }
              }
          }
          stage('My Test Name 2') {
              agent {
                  docker {
                      image 'noisepage:focal'
                      args '--cap-add sys_ptrace -v /jenkins/ccache:/home/jenkins/.ccache'
                  }
              }
              environment {
                  <!-- Add environment variables here -->
              }
              steps {
                  <!-- Add a list of shell commands here -->
              }
              post {
                  cleanup {
                      deleteDir()
                  }
              }
          }
      }
  }
  ```