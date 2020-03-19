#!/usr/bin/python
import os
from datetime import datetime

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
DEFAULT_TEST_OUTPUT_FILE = "/tmp/terrier_test_{}.log".format(
    datetime.utcnow().isoformat(sep="-", timespec="seconds").replace(":", "-"))

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
OLTP_DIR_CONFIG = os.path.join(OLTP_GIT_LOCAL_PATH, "config")

#oltp default settings
OLTP_DEFAULT_TIME = 20
OTLP_DEFAULT_TERMINAL = 1
OLTP_DEFAULT_SCALEFACTOR = 1
OLTP_DEFAULT_TRANSACTION_ISOLATION = "TRANSACTION_SERIALIZABLE"
OLTP_DEFAULT_USERNAME = "postgres"
OLTP_DEFAULT_PASSWORD = "postgres"
OLTP_DEFAULT_DBTYPE = "noisepage"
OLTP_DEFAULT_DRIVER = "org.postgresql.Driver"
OLTP_DEFAULT_RATE = "unlimited"
OLTP_DEFAULT_BIN = os.path.join(OLTP_GIT_LOCAL_PATH, "oltpbenchmark")
OLTP_DEFAULT_COMMAND_FLAGS = "--histograms  --create=true --load=true --execute=true -s 5"
OLTP_DIR_TEST_RESULT = os.path.join(OLTP_GIT_LOCAL_PATH, "results")


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1