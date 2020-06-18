# Testing Scripts

## Folder structure
- `util`(compatible with python3): all the common utilities for running all kinds of tests
- `oltpbench`(compatible with python3): entry script to fire an oltp bench test
- `junit`(compatible with python3): entry script to fire a junit test (and many other supporting configs)
- `jdbc`(legacy shell script): entry script to fire a jdbc test (and many other supporting configs)

## Util
`util` folder contains a list of common Python scripts
- `TestServer`: the base class for running all types of tests
- `TestJUnit`: the test class for running junit tests
- `TestOLTPBench`: the test class for configuring oltp and running oltp bench tests
- `constants`: all the constants used in the any file under the `util`

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
- prepare the DB
  - check if the terrier bin exists
  - fork a subprocess to start the DB (via python subprocess.Popen)
- run the pre-test task (test specific)
- run the test
  - fork a subprocess to start the test process using the command (via python subprocess.Popen)
  - check the return code from the OS
  - write the stdout and the stderr to the test output log file
- run the post-test task (test specific)
- stop the DB
- print out the logs to the stdout

### Adding a new test class
All test classes should inherit from the `TestServer` class. Anyone is free to modify any attribute from the base class.
- Mandatory attributes
  - `test_command` (`List(str)`): the command to run the test
- Optional attributes
  - `test_command_cwd` (`str`): the working directory to run the test command
  - `test_error_msg` (`str`): the error message to display in case of errors
- Optional functions
  - `run_pre_test`: the pre-test tasks required for the test
    - e.g. install oltp bin, config the xml file, etc.
  - `run_post_test`: the post-test tasks required for the test
    - e.g. parse the output json, etc.