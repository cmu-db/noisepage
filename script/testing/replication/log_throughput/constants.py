# Server Arg Keys
import sys

BUILD_TYPE_KEY = "build_type"
SERVER_ARGS_KEY = "server_args"
PORT_KEY = "port"
MESSENGER_PORT_KEY = "messenger_port"
REPLICATION_PORT_KEY = "replication_port"
MESSENGER_ENABLED_KEY = "messenger_enable"
REPLICATION_ENABLED_KEY = "replication_enable"
NETWORK_IDENTITY_KEY = "network_identity"
ASYNC_REPLICATION_KEY = "async_replication_enable"
WAL_ASYNC_COMMIT_KEY = "wal_async_commit_enable"
METRICS_KEY = "metrics"
USE_METRICS_THREAD_KEY = "use_metrics_thread"
LOGGING_METRICS_ENABLED_KEY = "logging_metrics_enable"
CONNECTION_THREAD_COUNT_KEY = "connection_thread_count"

# Server Arg Default Values
DEFAULT_BUILD_TYPE = "release"
REPLICATION_HOSTS_PATH = "../../script/testing/replication/log_throughput/resources/replication.config"
# Tuned to maximize log throughput under async durability and no replication on the dev10 machine
DEFAULT_CONNECTION_THREADS = 140
DEFAULT_RECORD_BUFFER_SEGMENT_SIZE = 10000000

# Server Args

DEFAULT_PRIMARY_SERVER_ARGS = {
    BUILD_TYPE_KEY: DEFAULT_BUILD_TYPE,
    SERVER_ARGS_KEY: {
        PORT_KEY: 15721,
        MESSENGER_PORT_KEY: 9022,
        REPLICATION_PORT_KEY: 15445,
        MESSENGER_ENABLED_KEY: False,
        REPLICATION_ENABLED_KEY: False,
        NETWORK_IDENTITY_KEY: "primary",
        "replication_hosts_path": REPLICATION_HOSTS_PATH,
        ASYNC_REPLICATION_KEY: False,
        "wal_enable": True,
        "wal_file_path": "wal-primary.log",
        WAL_ASYNC_COMMIT_KEY: False,
        METRICS_KEY: True,
        USE_METRICS_THREAD_KEY: True,
        LOGGING_METRICS_ENABLED_KEY: True,
        CONNECTION_THREAD_COUNT_KEY: DEFAULT_CONNECTION_THREADS,
        "record_buffer_segment_size": DEFAULT_RECORD_BUFFER_SEGMENT_SIZE
    }
}

DEFAULT_REPLICA_SERVER_ARGS = {
    BUILD_TYPE_KEY: DEFAULT_BUILD_TYPE,
    SERVER_ARGS_KEY: {
        PORT_KEY: 15722,
        MESSENGER_PORT_KEY: 9023,
        REPLICATION_PORT_KEY: 15446,
        MESSENGER_ENABLED_KEY: True,
        REPLICATION_ENABLED_KEY: True,
        NETWORK_IDENTITY_KEY: "replica",
        "replication_hosts_path": REPLICATION_HOSTS_PATH,
        ASYNC_REPLICATION_KEY: False,
        "wal_enable": True,
        "wal_file_path": "wal-replica.log",
        WAL_ASYNC_COMMIT_KEY: False,
        METRICS_KEY: False,
        USE_METRICS_THREAD_KEY: False,
        LOGGING_METRICS_ENABLED_KEY: False,
        CONNECTION_THREAD_COUNT_KEY: DEFAULT_CONNECTION_THREADS,
        "record_buffer_segment_size": DEFAULT_RECORD_BUFFER_SEGMENT_SIZE
    }
}

# OLTP Keys and Values
YCSB = "ycsb"
TPCC = "tpcc"
TATP = "tatp"

BENCHMARK_KEY = "benchmark"
DEFAULT_BENCHMARK = YCSB
TERMINALS_KEY = "terminals"
LOADER_THREADS_KEY = "loader_threads"
SCALE_FACTOR_KEY = "scale_factor"
# Tuned to maximize log throughput under async durability and no replication on the dev10 machine
DEFAULT_SCALE_FACTOR = 3000
WEIGHTS_KEY = "weights"

# Weights for different benchmarks. not needed for the load phase, but it is validated so we need some valid values
YCSB_WEIGHTS = "50,5,15,10,10,10"
TPCC_WEIGHTS = "45,43,4,4,4"
TATP_WEIGHTS = "2,35,10,35,2,14,2"
WEIGHTS_MAP = {
    YCSB: YCSB_WEIGHTS,
    TPCC: TPCC_WEIGHTS,
    TATP: TATP_WEIGHTS
}

# OLTP config
DEFAULT_OLTP_TEST_CASE = {
    BENCHMARK_KEY: DEFAULT_BENCHMARK,
    "query_mode": "extended",
    TERMINALS_KEY: DEFAULT_CONNECTION_THREADS,
    SCALE_FACTOR_KEY: DEFAULT_SCALE_FACTOR,
    WEIGHTS_KEY: WEIGHTS_MAP[DEFAULT_BENCHMARK],
    "client_time": 60,
    LOADER_THREADS_KEY: DEFAULT_CONNECTION_THREADS
}

# Log record messages
# Scraped from the first couple of logs of the load phase of TPCC
DEFAULT_LOG_RECORD_MESSAGES_FILE = "script/testing/replication/log_throughput/resources/log-messages-small.txt"

# Metrics files info
LOG_SERIALIZER_CSV = "log_serializer_task.csv"
DISK_LOG_CONSUMER_CSV = "disk_log_consumer_task.csv"
RECOVERY_MANAGER_CSV = "recovery_manager.csv"
METRICS_FILES = {LOG_SERIALIZER_CSV, DISK_LOG_CONSUMER_CSV, RECOVERY_MANAGER_CSV}

RESULTS_DIR = "script/testing/replication/log_throughput/results"

METRICS_NUM_RECORDS_COL = "num_records"
METRICS_START_TIME_COL = "start_time"
METRICS_ELAPSED_TIME_COL = "elapsed_us"

# Information for writing and reading messages to files
ENDIAN = "big"
# Length in bytes of the size of the next message
SIZE_LENGTH = sys.getsizeof(int)

# Misc
UTF_8 = "utf-8"
