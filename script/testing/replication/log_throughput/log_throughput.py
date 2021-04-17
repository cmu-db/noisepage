import os
import time
from typing import List

import pandas as pd

from .constants import *
from .metrics_file_util import get_results_dir, delete_metrics_file, create_results_dir, \
    move_metrics_file_to_results_dir, delete_metrics_files
from .node_server import NodeServer, PrimaryNode, ReplicaNode
from .test_type import TestType

"""
This file helps to generate load for the a NoisePage server. Then using the metrics collection framework will calculate 
the log throughput for that server. 
"""


def log_throughput(test_type: TestType, build_type: str, replication_enabled: bool, async_commit: bool,
                   oltp_benchmark: str, log_messages_file: str, connection_threads: int, output_file: str):
    """
    Measures the log throughput of a NoisePage server. Can measure either a primary or replica node. For primary nodes
    we can test with replication on or off.

    For both primary and replica nodes we utilize logging metrics to keep track of how many log records were created or
    applied. At the end of the test we aggregate these metrics to calculate average log throughput.

    For primary nodes we generate a write heavy workload using the load phase of an OLTP Benchmark.
    For replica nodes we manually send log records to a replica node using the LogShipper. The LogScraper can help with
    collecting log records for shipping.
    # TODO automate the process of generating logs

    :param test_type Indicates whether to measure throughput on primary or replica nodes
    :param build_type The type of build for the server
    :param replication_enabled Whether or not replication is enabled (only relevant when test_type is PRIMARY)
    :param async_commit Whether or not async commit is enabled
    :param oltp_benchmark Which OLTP benchmark to run (only relevant when test_type is PRIMARY)
    :param log_messages_file File containing log record messages to send to the replica (only relevant when test_type
           is REPLICA)
    :param output_file Where to save the metrics to
    """

    if output_file is None:
        output_file = f"{test_type.value}-log-throughput-{int(time.time())}.csv"

    metrics_file = LOG_SERIALIZER_CSV if test_type.value == TestType.PRIMARY.value else RECOVERY_MANAGER_CSV
    other_metrics_files = METRICS_FILES
    other_metrics_files.remove(metrics_file)

    servers = get_servers(test_type, build_type, replication_enabled, async_commit, oltp_benchmark, log_messages_file,
                          connection_threads)

    try:
        for server in servers:
            server.setup()
    except RuntimeError as e:
        print(e)
        return

    # We don't care about log records generated at startup
    delete_metrics_file(metrics_file)

    for server in servers:
        server.run()

    for server in servers:
        server.teardown()

    create_results_dir()

    delete_metrics_files(other_metrics_files)
    move_metrics_file_to_results_dir(metrics_file, output_file)

    aggregate_log_throughput(output_file)


def get_servers(test_type: TestType, build_type: str, replication_enabled: bool, async_commit: bool,
                oltp_benchmark: str, log_messages_file: str, connection_threads: int) -> List[NodeServer]:
    """
    Creates server instances for the log throughput test

    :param test_type Indicates whether to measure throughput on primary or replica nodes
    :param build_type The type of build for the server
    :param replication_enabled Whether or not replication is enabled (only relevant when test_type is PRIMARY)
    :param async_commit Whether or not async commit is enabled
    :param oltp_benchmark Which OLTP benchmark to run (only relevant when test_type is PRIMARY)
    :param log_messages_file File containing log record messages to send to the replica (only relevant when test_type
           is REPLICA)
    :param connection_threads How many database connection threads to use

    :return list of server instances
    """
    servers = []
    if test_type.value == TestType.PRIMARY.value:
        servers.append(PrimaryNode(build_type, replication_enabled, async_commit, oltp_benchmark, connection_threads))
        if replication_enabled:
            servers.append(ReplicaNode(test_type, build_type, async_commit, log_messages_file, connection_threads))
    elif test_type.value == TestType.REPLICA.value:
        servers.append(ReplicaNode(test_type, build_type, async_commit, log_messages_file, connection_threads))
    return servers


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

    if df.shape[0] <= 1:
        print("Not enough data to calculate log throughput")
        return

    # Microseconds
    end_time = df.iloc[-1][METRICS_START_TIME_COL] + df.iloc[-1][METRICS_ELAPSED_TIME_COL]
    start_time = df.iloc[0][METRICS_START_TIME_COL]
    total_time = end_time - start_time

    total_records = df[METRICS_NUM_RECORDS_COL].sum()

    # Convert to milliseconds
    avg_throughput = (total_records / total_time) * 1000

    print(f"Average log throughput is {avg_throughput} per millisecond")
