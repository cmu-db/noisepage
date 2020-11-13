#!/usr/bin/python3
import os
from datetime import datetime
from util.constants import DIR_TMP

# git settings of OLTP
OLTPBENCH_GIT_URL = "https://github.com/oltpbenchmark/oltpbench.git"
OLTPBENCH_GIT_LOCAL_PATH = os.path.join(DIR_TMP, "oltpbench")
OLTPBENCH_GIT_CLEAN_COMMAND = "rm -rf {}".format(OLTPBENCH_GIT_LOCAL_PATH)
OLTPBENCH_GIT_COMMAND = "git clone {} {}".format(OLTPBENCH_GIT_URL,
                                                 OLTPBENCH_GIT_LOCAL_PATH)

# oltp default settings
OLTPBENCH_DEFAULT_TIME = 30
OLTPBENCH_DEFAULT_TERMINALS = 1
OLTPBENCH_DEFAULT_LOADER_THREADS = 1
OLTPBENCH_DEFAULT_SCALEFACTOR = 1
OLTPBENCH_DEFAULT_CONNECTION_THREAD_COUNT = 4
OLTPBENCH_DEFAULT_TRANSACTION_ISOLATION = "TRANSACTION_SERIALIZABLE"
OLTPBENCH_DEFAULT_USERNAME = "postgres"
OLTPBENCH_DEFAULT_PASSWORD = "postgres"
OLTPBENCH_DEFAULT_DBTYPE = "noisepage"
OLTPBENCH_DEFAULT_DRIVER = "org.postgresql.Driver"
OLTPBENCH_DEFAULT_RATE = "unlimited"
OLTPBENCH_DEFAULT_BIN = os.path.join(OLTPBENCH_GIT_LOCAL_PATH, "oltpbenchmark")
OLTPBENCH_DEFAULT_DATABASE_RESTART = True
OLTPBENCH_DEFAULT_DATABASE_CREATE = True
OLTPBENCH_DEFAULT_DATABASE_LOAD = True
OLTPBENCH_DEFAULT_DATABASE_EXECUTE = True
OLTPBENCH_DEFAULT_REPORT_SERVER = None
OLTPBENCH_DEFAULT_WAL_ENABLE = True
OLTPBENCH_DEFAULT_CONTINUE_ON_ERROR = False

OLTPBENCH_DIR_CONFIG = os.path.join(OLTPBENCH_GIT_LOCAL_PATH, "config")
OLTPBENCH_DIR_TEST_RESULT = os.path.join(OLTPBENCH_GIT_LOCAL_PATH, "results")
OLTPBENCH_TEST_ERROR_MSG = "Error: failed to complete oltpbench test"

# ant commands for OLTP
OLTPBENCH_ANT_BUILD_FILE = os.path.join(OLTPBENCH_GIT_LOCAL_PATH, "build.xml")
OLTPBENCH_ANT_COMMAND_BOOTSTRAP = "ant bootstrap -buildfile {}".format(
    OLTPBENCH_ANT_BUILD_FILE)
OLTPBENCH_ANT_COMMAND_RESOLVE = "ant resolve -buildfile {}".format(
    OLTPBENCH_ANT_BUILD_FILE)
OLTPBENCH_ANT_CLEAN = "ant clean -buildfile {}".format(
    OLTPBENCH_ANT_BUILD_FILE)
OLTPBENCH_ANT_COMMAND_BUILD = "ant build -buildfile {}".format(
    OLTPBENCH_ANT_BUILD_FILE)
OLTPBENCH_ANT_COMMANDS = [
    OLTPBENCH_ANT_COMMAND_BOOTSTRAP, OLTPBENCH_ANT_COMMAND_RESOLVE,
    OLTPBENCH_ANT_CLEAN, OLTPBENCH_ANT_COMMAND_BUILD
]

# API endpoints for Performance Storage Service
# Each pair represents different environment. One could choose where the benchmark testing result will be uploaded to
# The default is none, which means that the testing result won't be uploaded to any server
PERFORMANCE_STORAGE_SERVICE_API = {
    "none": "",
    "test": "https://incrudibles-testing.db.pdl.cmu.edu/performance-results",
    "staging":
    "https://incrudibles-staging.db.pdl.cmu.edu/performance-results",
    "prod": "https://incrudibles-production.db.pdl.cmu.edu/performance-results"
}