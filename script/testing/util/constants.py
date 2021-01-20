#!/usr/bin/python3
import os
from datetime import datetime
import logging

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
DEFAULT_DB_BIN = "noisepage"
DEFAULT_DB_USER = "noisepage"
DEFAULT_TEST_OUTPUT_FILE = "/tmp/noisepage_test_{}.log".format(
    datetime.utcnow().isoformat(sep="-", timespec="seconds").replace(":", "-"))
DEFAULT_DB_WAL_FILE = "wal.log"
# Whether the database should stop the whole test if one of test cases fail,
DEFAULT_CONTINUE_ON_ERROR = False

# Number of times we will try to start the DBMS and connect to it
DB_START_ATTEMPTS = 6

# Logging settings
LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s,%(msecs)03d [%(filename)s:%(lineno)d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

# API endpoints for Performance Storage Service
# Each pair represents different environment. One could choose where the benchmark testing result will be uploaded to
# The default is none, which means that the testing result won't be uploaded to any server
PERFORMANCE_STORAGE_SERVICE_API = {
    "none": "",
    "local": "http://host.docker.internal:8000/performance-results",
    "test": "https://incrudibles-testing.db.pdl.cmu.edu/performance-results",
    "staging":
    "https://incrudibles-staging.db.pdl.cmu.edu/performance-results",
    "prod": "https://incrudibles-production.db.pdl.cmu.edu/performance-results"
}
# Scripts with psutils
FILE_CHECK_PIDS = os.path.join(DIR_TESTING, "check_pids.py")
FILE_KILL_SERVER = os.path.join(DIR_TESTING, "kill_server.py")
FILE_COLLECT_MEM_INFO = os.path.join(DIR_TESTING, "collect_mem_info.py")

# Command paths
LSOF_PATH_LINUX = "lsof"
LSOF_PATH_MACOS = "/usr/sbin/lsof"

# OS family
OS_FAMILY_DARWIN = "darwin"

# Memory info collection
MEM_INFO_SPLITTER = ","

# Incremental metrics
INCREMENTAL_METRIC_FREQ = 5  # collect incremental metrics every 5 seconds by default


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1


# psutil command line strings
class CommandLineStr:
    TRUE = "TRUE"
    FALSE = "FALSE"
