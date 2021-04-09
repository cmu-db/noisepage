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
This file helps generate load for the a primary NoisePage server. Then using the metrics collection framework will 
calculate the log throughput for the primary server. 
"""


def primary_log_throughput(test_type: TestType, build_type: str, replication_enabled: bool, oltp_benchmark: str,
                           output_file: str):
    """
    Measures the log throughput of the primary server.

    We accomplish this by generates a write heavy workload using the load phase of an OLTP Benchmark. The server is run
    with logging metrics turned on so that we can analyze metrics related to logging. Once OLTP is done loading data,
    we calculate the average log throughput from the logging metrics.

    :param test_type Indicates whether to measure throughput on primary/replica. Valid values are {'primary', 'replica'}
    :param build_type The type of build for the server
    :param replication_enabled Whether or not replication is enabled
    :param oltp_benchmark Which OLTP benchmark to run
    :param output_file Where to save the metrics to
    """

    if output_file is None:
        output_file = f"{test_type.value}-log-throughput-{int(time.time())}.csv"

    metrics_file = LOG_SERIALIZER_CSV if test_type.value == TestType.PRIMARY.value else RECOVERY_MANAGER_CSV
    other_metrics_files = METRICS_FILES
    other_metrics_files.remove(metrics_file)

    servers = get_servers(test_type, build_type, replication_enabled, oltp_benchmark)

    for server in servers:
        server.setup()

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


def get_servers(test_type: TestType, build_type: str, replication_enabled: bool, oltp_benchmark: str) -> \
        List[NodeServer]:
    """
    Creates server instances for the log throughput test

    :param test_type Indicates whether to measure throughput on primary/replica. Valid values are {'primary', 'replica'}
    :param build_type The type of build for the server
    :param replication_enabled Whether or not replication is enabled
    :param oltp_benchmark Which OLTP benchmark to run

    :return list of server instances
    """
    servers = []
    if test_type.value == TestType.PRIMARY.value:
        servers.append(PrimaryNode(build_type, replication_enabled, oltp_benchmark))
        if replication_enabled:
            servers.append(ReplicaNode(test_type, build_type))
    elif test_type.value == TestType.REPLICA.value:
        servers.append(ReplicaNode(test_type, build_type))
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

    # Microseconds
    end_time = df.iloc[-1][METRICS_START_TIME_COL] + df.iloc[-1][METRICS_ELAPSED_TIME_COL]
    start_time = df.iloc[0][METRICS_START_TIME_COL]
    total_time = end_time - start_time

    total_records = df[METRICS_NUM_RECORDS_COL].sum()

    # Convert to milliseconds
    avg_throughput = (total_records / total_time) * 1000

    print(f"Average log throughput is {avg_throughput} per millisecond")
