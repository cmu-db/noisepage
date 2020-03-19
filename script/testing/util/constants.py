#!/usr/bin/python3
import os
from datetime import datetime

# absolute paths
DIR_UTIL = os.path.dirname(os.path.realpath(__file__))
DIR_TESTING = os.path.dirname(DIR_UTIL)
DIR_SCRIPT = os.path.dirname(DIR_TESTING)
DIR_REPO = os.path.dirname(DIR_SCRIPT)
DIR_TMP = "/tmp"

# default settings of the TestServer
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 15721
DEFAULT_DB_OUTPUT_FILE = "/tmp/db_log.txt"
DEFAULT_DB_BIN = "terrier"
DEFAULT_TEST_OUTPUT_FILE = "/tmp/terrier_test_{}.log".format(
    datetime.utcnow().isoformat(sep="-", timespec="seconds").replace(":", "-"))

# settings of TestJUnit
JUNIT_TEST_DIR = os.path.join(DIR_TESTING, "junit")
JUNIT_TEST_COMMAND = "ant testconsole"
JUNIT_TEST_ERROR_MSG = "Error: failed to complete junit test"

# git settings of OLTP
OLTP_GIT_URL = "https://github.com/oltpbenchmark/oltpbench.git"
OLTP_GIT_LOCAL_PATH = os.path.join(DIR_TMP, "oltpbench")
OLTP_GIT_CLEAN_COMMAND = "rm -rf {}".format(OLTP_GIT_LOCAL_PATH)
OLTP_GIT_COMMAND = "git clone {} {}".format(OLTP_GIT_URL, OLTP_GIT_LOCAL_PATH)

# oltp default settings
OLTP_DEFAULT_TIME = 20
OLTP_DEFAULT_TERMINALS = 1
OLTP_DEFAULT_SCALEFACTOR = 1
OLTP_DEFAULT_TRANSACTION_ISOLATION = "TRANSACTION_SERIALIZABLE"
OLTP_DEFAULT_USERNAME = "postgres"
OLTP_DEFAULT_PASSWORD = "postgres"
OLTP_DEFAULT_DBTYPE = "noisepage"
OLTP_DEFAULT_DRIVER = "org.postgresql.Driver"
OLTP_DEFAULT_RATE = "unlimited"
OLTP_DEFAULT_BIN = os.path.join(OLTP_GIT_LOCAL_PATH, "oltpbenchmark")
OLTP_DEFAULT_COMMAND_FLAGS = "--histograms  --create=true --load=true --execute=true -s 5"
OLTP_DIR_CONFIG = os.path.join(OLTP_GIT_LOCAL_PATH, "config")
OLTP_DIR_TEST_RESULT = os.path.join(OLTP_GIT_LOCAL_PATH, "results")
OLTP_TEST_ERROR_MSG = "Error: failed to complete oltpbench test"

# ant commands for OLTP
OLTP_ANT_BUILD_FILE = os.path.join(OLTP_GIT_LOCAL_PATH, "build.xml")
OTLP_ANT_COMMAND_BOOTSTRAP = "ant bootstrap -buildfile {}".format(
    OLTP_ANT_BUILD_FILE)
OTLP_ANT_COMMAND_RESOLVE = "ant resolve -buildfile {}".format(
    OLTP_ANT_BUILD_FILE)
OTLP_ANT_COMMAND_BUILD = "ant build -buildfile {}".format(OLTP_ANT_BUILD_FILE)
OTLP_ANT_COMMANDS = [
    OTLP_ANT_COMMAND_BOOTSTRAP, OTLP_ANT_COMMAND_RESOLVE,
    OTLP_ANT_COMMAND_BUILD
]


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1