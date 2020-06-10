#!/usr/bin/python3
import os
from datetime import datetime
from util.constants import DIR_TMP

# git settings of OLTP
OLTP_GIT_URL = "https://github.com/oltpbenchmark/oltpbench.git"
OLTP_GIT_LOCAL_PATH = os.path.join(DIR_TMP, "oltpbench")
OLTP_GIT_CLEAN_COMMAND = "rm -rf {}".format(OLTP_GIT_LOCAL_PATH)
OLTP_GIT_COMMAND = "git clone {} {}".format(OLTP_GIT_URL, OLTP_GIT_LOCAL_PATH)

# oltp default settings
OLTP_DEFAULT_TIME = 30
OLTP_DEFAULT_TERMINALS = 1
OLTP_DEFAULT_LOADER_THREADS = 1
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
OLTP_ANT_COMMAND_BOOTSTRAP = "ant bootstrap -buildfile {}".format(
    OLTP_ANT_BUILD_FILE)
OLTP_ANT_COMMAND_RESOLVE = "ant resolve -buildfile {}".format(
    OLTP_ANT_BUILD_FILE)
OLTP_ANT_COMMAND_BUILD = "ant build -buildfile {}".format(OLTP_ANT_BUILD_FILE)
OLTP_ANT_COMMANDS = [
    OLTP_ANT_COMMAND_BOOTSTRAP, OLTP_ANT_COMMAND_RESOLVE,
    OLTP_ANT_COMMAND_BUILD
]
