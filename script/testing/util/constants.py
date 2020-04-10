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

DB_START_ATTEMPTS = 3
DB_CONNECT_ATTEMPTS = 100
DB_CONNECT_SLEEP = 0.1 # seconds


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1
