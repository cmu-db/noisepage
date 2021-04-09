BUILD_TYPE_KEY = "build_type"
SERVER_ARGS_KEY = "server_args"
PORT_KEY = "port"
MESSENGER_ENABLED_KEY = "messenger_enable"
REPLICATION_ENABLED_KEY = "replication_enable"

REPLICATION_HOSTS_PATH = "../../script/testing/replication/log_throughput/replication.config"

DEFAULT_PRIMARY_SERVER_ARGS = {
    BUILD_TYPE_KEY: "release",
    SERVER_ARGS_KEY: {
        PORT_KEY: 15721,
        "messenger_port": 9022,
        "replication_port": 15445,
        MESSENGER_ENABLED_KEY: False,
        REPLICATION_ENABLED_KEY: False,
        "network_identity": "primary",
        "replication_hosts_path": REPLICATION_HOSTS_PATH,
        "wal_enable": True,
        "wal_file_path": "wal-primary.log",
        "metrics": True,
        "use_metrics_thread": True,
        "logging_metrics_enable": True,
        "connection_thread_count": 32,
        "record_buffer_segment_size": 10000000
    }
}

DEFAULT_REPLICA_SERVER_ARGS = {
    BUILD_TYPE_KEY: "release",
    SERVER_ARGS_KEY: {
        "port": 15722,
        "messenger_port": 9023,
        "replication_port": 15446,
        MESSENGER_ENABLED_KEY: True,
        REPLICATION_ENABLED_KEY: True,
        "network_identity": "replica",
        "replication_hosts_path": REPLICATION_HOSTS_PATH,
        "wal_enable": True,
        "wal_file_path": "wal-replica.log",
        "metrics": False,
        "use_metrics_thread": False,
        "logging_metrics_enable": False,
        "connection_thread_count": 32,
        "record_buffer_segment_size": 10000000
    }
}

BENCHMARK_KEY = "benchmark"

DEFAULT_OLTP_TEST_CASE = {
    BENCHMARK_KEY: "tpcc",
    "query_mode": "extended",
    "terminals": 32,
    "scale_factor": 1,
    "weights": "45,43,4,4,4",
    "client_time": 60,
    "loader_threads": 32
}

LOG_SERIALIZER_CSV = "log_serializer_task.csv"
DISK_LOG_CONSUMER_CSV = "disk_log_consumer_task.csv"
RECOVERY_MANAGER_CSV = "recovery_manager.csv"

RESULTS_DIR = "script/testing/replication/log_throughput/results"

METRICS_NUM_RECORDS_COL = "num_records"
METRICS_ELAPSED_TIME_COL = "elapsed_us"
