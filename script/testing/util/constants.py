#!/usr/bin/python
import os

# absolute paths
DIR_UTIL = os.path.dirname(os.path.realpath(__file__))
DIR_TESTING = os.path.dirname(DIR_UTIL)
DIR_SCRIPT = os.path.dirname(DIR_TESTING)
DIR_REPO = os.path.dirname(DIR_SCRIPT)

# default settings of the TestServer
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 15721
DEFAULT_DB_OUTPUT_FILE = "/tmp/db_log.txt"
DEFAULT_DB_BIN = "terrier"

# settings of TestJUnit
JUNIT_DB_OUTPUT_FILE = "/tmp/junit_log.txt"
JUNIT_TEST_COMMAND = "ant testconsole".split(" ")

# ant commands for OLTP
OTLP_ANT_COMMAND_BOOTSTRAP = "ant bootstrap".split(" ")
OTLP_ANT_COMMAND_RESOLVE = "ant resolve".split(" ")
OTLP_ANT_COMMAND_BUILD = "ant build".split(" ")
OTLP_ANT_COMMANDS = [
    OTLP_ANT_COMMAND_BOOTSTRAP, OTLP_ANT_COMMAND_RESOLVE,
    OTLP_ANT_COMMAND_BUILD
]

# git settings of OLTP
OLTP_GIT_URL = "https://github.com/oltpbenchmark/oltpbench.git"
OLTP_GIT_LOCAL_PATH = os.path.join(DIR_REPO, "oltpbench")
OLTP_GIT_COMMAND = "git clone {} OLTP_GIT_LOCAL_PATH".format(
    OLTP_GIT_URL).split((" "))


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1