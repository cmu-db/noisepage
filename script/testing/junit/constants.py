#!/usr/bin/python3
import os
from util.constants import DIR_TESTING

# settings of TestJUnit
JUNIT_TEST_DIR = os.path.join(DIR_TESTING, "junit")
JUNIT_TEST_COMMAND = "ant testconsole"
JUNIT_TEST_ERROR_MSG = "Error: failed to complete junit test"
JUNIT_OUTPUT_FILE = "/tmp/junit_log.txt"
