import os

import pandas as pd

from .constants import *
from .metrics_file_util import get_results_dir, delete_metrics_file, create_results_dir, move_metrics_file
from ...oltpbench.test_case_oltp import TestCaseOLTPBench
from ...oltpbench.test_oltpbench import TestOLTPBench
from ...util.db_server import NoisePageServer


def primary_log_throughput(build_type, replication_enabled, oltp_benchmark):
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
    move_metrics_file(LOG_SERIALIZER_CSV)

    aggregate_log_throughput(LOG_SERIALIZER_CSV)


def start_replica(build_type):
    replica_server_args = DEFAULT_REPLICA_SERVER_ARGS
    replica_server_args[BUILD_TYPE_KEY] = build_type
    replica = NoisePageServer(build_type=build_type,
                              port=replica_server_args[SERVER_ARGS_KEY][PORT_KEY],
                              server_args=replica_server_args[SERVER_ARGS_KEY])
    replica.run_db()
    return replica


def aggregate_log_throughput(file_name):
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
