import os

from ..util.constants import DIR_TMP

# git settings for OLTPBench.
OLTPBENCH_VERSION = "oltpbench2-20.1.3-SNAPSHOT"
OLTPBENCH_GIT_URL = "https://github.com/oltpbenchmark/oltpbench.git"
OLTPBENCH_GIT_LOCAL_PATH = os.path.join(DIR_TMP, "benchbase")
OLTPBENCH_GIT_TARGET_PATH = os.path.join(OLTPBENCH_GIT_LOCAL_PATH, "target")
OLTPBENCH_GIT_FINAL_PATH = os.path.join(OLTPBENCH_GIT_TARGET_PATH, OLTPBENCH_VERSION)
OLTPBENCH_GIT_CLEAN_COMMAND = "rm -rf {}".format(OLTPBENCH_GIT_LOCAL_PATH)
OLTPBENCH_GIT_CLONE_COMMAND = "git clone --single-branch --branch oltpbench_tim --depth 1 {} {}".format(
    OLTPBENCH_GIT_URL,
    OLTPBENCH_GIT_LOCAL_PATH)

# OLTPBench default settings.
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
OLTPBENCH_DEFAULT_BIN = "java -jar oltpbench2.jar "
OLTPBENCH_DEFAULT_DATABASE_RESTART = True
OLTPBENCH_DEFAULT_DATABASE_CREATE = True
OLTPBENCH_DEFAULT_DATABASE_LOAD = True
OLTPBENCH_DEFAULT_DATABASE_EXECUTE = True
OLTPBENCH_DEFAULT_REPORT_SERVER = None
OLTPBENCH_DEFAULT_WAL_ENABLE = True
OLTPBENCH_DEFAULT_CONTINUE_ON_ERROR = False

OLTPBENCH_DIR_CONFIG = os.path.join(OLTPBENCH_GIT_FINAL_PATH, "config", "noisepage")
OLTPBENCH_DIR_TEST_RESULT = os.path.join(OLTPBENCH_GIT_FINAL_PATH, "results")

# API endpoints for Performance Storage Service
# Each pair represents different environment. One could choose where the benchmark testing result will be uploaded to
# The default is none, which means that the testing result won't be uploaded to any server
PERFORMANCE_STORAGE_SERVICE_API = {
    "none": "",
    "test": "https://incrudibles-testing.db.pdl.cmu.edu/performance-results",
    "staging": "https://incrudibles-staging.db.pdl.cmu.edu/performance-results",
    "prod": "https://incrudibles-production.db.pdl.cmu.edu/performance-results"
}
