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

# Number of seconds to wait after starting the DBMS before trying to connect
DB_START_WAIT = 2 # seconds
# Number of times we will try to start the DBMS and connect to it
DB_START_ATTEMPTS = 3
# For each start attempt, the number of times we will attempt to connect to the DBMS
DB_CONNECT_ATTEMPTS = 50
# How long to wait before each connection attempt
DB_CONNECT_SLEEP = 0.2 # seconds


# error code
class ErrorCode:
    SUCCESS = 0
    ERROR = 1
