import json
from time import time

from ...utils import get_value_by_pattern
from ...constants import UNKNOWN_RESULT, LATENCY_ATTRIBUTE_MAPPING


def parse_summary_file(path):
    """
    Read data from an OLTPBench-generated summary file.

    Parameters
    ----------
    path : str
        Path to an OLTPBench-generated summary file.

    Returns
    -------
    metadata : dict
        An object containing metadata information.
    timestamp : int
        The timestamp when the benchmark was created, in milliseconds.
        TODO(WAN): wtf is this?
    benchmark_type : str
        The benchmark that was run (e.g., tatp, noop).
    parameters : dict
        Information about the parameters with which the test was run.
    metrics : dict
        The summary measurements that were gathered from the test.
    """
    gvbp = get_value_by_pattern

    def get_latency_val(latency_dist, pattern):
        value = gvbp(latency_dist, pattern, None)
        return float("{:.4}".format(value)) if value else value

    with open(path) as summary_file:
        summary = json.load(summary_file)
        latency_dist = summary.get('Latency Distribution', {})

        metadata = {
            'noisepage': {
                'db_version': summary.get('DBMS Version', UNKNOWN_RESULT)
            }
        }
        timestamp = int(gvbp(summary, 'timestamp', str(time())))
        benchmark_type = summary.get('Benchmark Type', UNKNOWN_RESULT)
        parameters = {
            'scale_factor': summary.get('scalefactor', '-1.0'),
            'terminals': int(summary.get('terminals', -1))
        }
        metrics = {
            'throughput': gvbp(summary, 'throughput', '-1.0'),
            'latency': {key: get_latency_val(latency_dist, pattern)
                        for key, pattern in LATENCY_ATTRIBUTE_MAPPING}
        }

        return metadata, timestamp, benchmark_type, parameters, metrics
