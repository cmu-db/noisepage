import os
import shutil

import pandas as pd

from ...oltpbench.test_case_oltp import TestCaseOLTPBench
from ...oltpbench.test_oltpbench import TestOLTPBench

PRIMARY_OLTP_SERVER_ARGS = {
    "build_type": "release",
    "server_args": {
        "wal_enable": True,
        "wal_file_path": "wal.log",
        "metrics": True,
        "use_metrics_thread": True,
        "logging_metrics_enable": True,
        "connection_thread_count": 32,
        "record_buffer_segment_size": 10000000
    }
}

OLTP_TEST_CASE = {
    "benchmark": "tpcc",
    "query_mode": "extended",
    "terminals": 32,
    "scale_factor": 32,
    "weights": "45,43,4,4,4",
    "client_time": 60,
    "loader_threads": 32
}

LOG_SERIALIZER_CSV = "log_serializer_task.csv"
DISK_LOG_CONSUMER_CSV = "disk_log_consumer_task.csv"
RECOVERY_MANAGER_CSV = "recovery_manager.csv"

RESULTS_DIR = "log_throughput_results"


def primary_log_throughput():
    # Start DB
    oltp_server = TestOLTPBench(PRIMARY_OLTP_SERVER_ARGS)
    db_server = oltp_server.db_instance
    db_server.run_db()

    # Download and prepare OLTP Bench
    oltp_server.run_pre_suite()

    # We don't care about log records generated at startup
    delete_metrics_file(LOG_SERIALIZER_CSV)

    # Load DB
    test_case = TestCaseOLTPBench(OLTP_TEST_CASE)
    test_case.run_pre_test()

    # Clean up, disconnect the DB
    db_server.stop_db()
    db_server.delete_wal()

    create_results_dir()

    delete_metrics_file(DISK_LOG_CONSUMER_CSV)
    delete_metrics_file(RECOVERY_MANAGER_CSV)
    move_metrics_file(LOG_SERIALIZER_CSV)

    aggregate_log_throughput(LOG_SERIALIZER_CSV)


def get_csv_file_path(file_name):
    return os.path.join(os.getcwd(), file_name)


def delete_metrics_file(file_name):
    path = get_csv_file_path(file_name)
    # It takes a little while for the metrics file to be created
    while not os.path.exists(path):
        pass
    os.remove(path)


def get_results_dir():
    return os.path.join(os.getcwd(), RESULTS_DIR)


def delete_results_dir():
    path = get_results_dir()
    if os.path.exists(path):
        shutil.rmtree(path)


def create_results_dir():
    delete_results_dir()
    os.makedirs(get_results_dir())


def move_metrics_file(file_name):
    path = get_csv_file_path(file_name)
    os.replace(path, os.path.join(get_results_dir(), file_name))


def aggregate_log_throughput(file_name):
    path = os.path.join(get_results_dir(), file_name)
    df = pd.read_csv(path)
    # remove first row because elapsed time isn't accurate since the DB has been idle for a bit
    df = df.iloc[1:]
    df = df.rename(columns=lambda col: col.strip())
    df["throughput"] = df["num_records"] / df["elapsed_us"]
    avg_throughput = df["throughput"].mean()
    print(f"Average log throughput is {avg_throughput * 1000} per millisecond")


def main():
    primary_log_throughput()


if __name__ == "__main__":
    main()
