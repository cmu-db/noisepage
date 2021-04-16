# Server Arg Keys
BUILD_TYPE_KEY = "build_type"
SERVER_ARGS_KEY = "server_args"
PORT_KEY = "port"
MESSENGER_PORT_KEY = "messenger_port"
REPLICATION_PORT_KEY = "replication_port"
MESSENGER_ENABLED_KEY = "messenger_enable"
REPLICATION_ENABLED_KEY = "replication_enable"
NETWORK_IDENTITY_KEY = "network_identity"
WAL_ASYNC_COMMIT_KEY = "wal_async_commit_enable"
METRICS_KEY = "metrics"
USE_METRICS_THREAD_KEY = "use_metrics_thread"
LOGGING_METRICS_ENABLED_KEY = "logging_metrics_enable"
CONNECTION_THREAD_COUNT_KEY = "connection_thread_count"

# Server Arg Default Values
DEFAULT_BUILD_TYPE = "release"
REPLICATION_HOSTS_PATH = "../../script/testing/replication/log_throughput/resources/replication.config"
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
BENCHMARK_KEY = "benchmark"
DEFAULT_BENCHMARK = "ycsb"
TERMINALS_KEY = "terminals"
LOADER_THREADS_KEY = "loader_threads"

# OLTP config
DEFAULT_OLTP_TEST_CASE = {
    BENCHMARK_KEY: DEFAULT_BENCHMARK,
    "query_mode": "extended",
    TERMINALS_KEY: DEFAULT_CONNECTION_THREADS,
    "scale_factor": 3000,
    "weights": "50,5,15,10,10,10",
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

# Misc
UTF_8 = "utf-8"
