import os

import pandas as pd

from .constants import *
from .metrics_file_util import get_results_dir, delete_metrics_file, create_results_dir, \
    move_metrics_file_to_results_dir
from ...oltpbench.test_case_oltp import TestCaseOLTPBench
from ...oltpbench.test_oltpbench import TestOLTPBench
from ...util.db_server import NoisePageServer


def primary_log_throughput(build_type: str, replication_enabled: bool, oltp_benchmark: str):
    """
    Measures the log throughput of the primary server.

    We accomplish this by generates a write heavy workload using the load phase of an OLTP Benchmark. The server is run
    with logging metrics turned on so that we can analyze metrics related to logging. Once OLTP is done loading data,
    we calculate the average log throughput from the logging metrics.

    :param build_type The type of build for the server
    :param  replication_enabled Whether or not replication is enabled
    :param oltp_benchmark Which OLTP benchmark to run

    """
    primary_server_args = DEFAULT_PRIMARY_SERVER_ARGS
    primary_server_args[BUILD_TYPE_KEY] = build_type

    replica = None
    if replication_enabled:
        replica = start_replica(build_type)
        primary_server_args[SERVER_ARGS_KEY][MESSENGER_ENABLED_KEY] = True
        primary_server_args[SERVER_ARGS_KEY][REPLICATION_ENABLED_KEY] = True

    # Start DB
    oltp_server = TestOLTPBench(primary_server_args)
    db_server = oltp_server.db_instance
    db_server.run_db()

    # Download and prepare OLTP Bench
    oltp_server.run_pre_suite()

    # We don't care about log records generated at startup
    delete_metrics_file(LOG_SERIALIZER_CSV)

    # Load DB
    oltp_test_case = DEFAULT_OLTP_TEST_CASE
    oltp_test_case[BENCHMARK_KEY] = oltp_benchmark
    test_case = TestCaseOLTPBench(oltp_test_case)
    test_case.run_pre_test()

    # Clean up, disconnect the DB
    db_server.stop_db()
    db_server.delete_wal()

    if replication_enabled:
        replica.stop_db()
        replica.delete_wal()

    create_results_dir()

    delete_metrics_file(DISK_LOG_CONSUMER_CSV)
    # delete_metrics_file(RECOVERY_MANAGER_CSV)
    move_metrics_file_to_results_dir(LOG_SERIALIZER_CSV)

    aggregate_log_throughput(LOG_SERIALIZER_CSV)


def start_replica(build_type: str) -> NoisePageServer:
    """
    Starts a replica server and returns instance

    :param build_type The type of build for the server
    """
    replica_server_args = DEFAULT_REPLICA_SERVER_ARGS
    replica_server_args[BUILD_TYPE_KEY] = build_type
    replica = NoisePageServer(build_type=build_type,
                              port=replica_server_args[SERVER_ARGS_KEY][PORT_KEY],
                              server_args=replica_server_args[SERVER_ARGS_KEY])
    replica.run_db()
    return replica


def aggregate_log_throughput(file_name: str):
    """
    Computes the average log throughput for a metrics file and prints the results

    :param file_name Name of metrics file
    """
    path = os.path.join(get_results_dir(), file_name)
    df = pd.read_csv(path)
    # remove first row because elapsed time isn't accurate since the DB has been idle for a bit
    df = df.iloc[1:]
    df = df.rename(columns=lambda col: col.strip())
    throughput_col = "throughput"
    # In microseconds
    df[throughput_col] = df[METRICS_NUM_RECORDS_COL] / df[METRICS_ELAPSED_TIME_COL]
    avg_throughput = df[throughput_col].mean()
    print(f"Average log throughput is {avg_throughput * 1000} per millisecond")
