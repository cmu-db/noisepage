import csv

from ...constants import LATENCY_ATTRIBUTE_MAPPING
from ...utils import get_value_by_pattern


def parse_res_file(path):
    """
    Read data from an OLTPBench-generated file that ends with ".res".

    Parameters
    ----------
    path : str
        Path to a ".res" file.

    Returns
    -------
    incremental_metrics : [dict]
        The throughput of the test at different times.
    """
    gvbp = get_value_by_pattern

    def get_latency_val(row, pattern):
        value = gvbp(row, pattern, None)
        return float("{:.4}".format(value)) if value else value

    incremental_metrics = []
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            incremental_metrics.append({
                "time": float(gvbp(row, 'time', None)),
                "throughput": float(gvbp(row, 'throughput', None)),
                "latency": {key: get_latency_val(row, pat)
                            for key, pat in LATENCY_ATTRIBUTE_MAPPING}
            })

    return incremental_metrics
