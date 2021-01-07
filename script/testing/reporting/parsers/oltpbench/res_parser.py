import csv

from ...constants import LATENCY_ATTRIBUTE_MAPPING
from ...utils import get_value_by_pattern


def parse_res_file(path):
    """Read data from file ends with ".res".

    Args:
        path (str): The position of the res file.

    Returns:
        incremental_metrics (list, json array): The throughput at different time.

    """
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        incremental_metrics = []
        for row in reader:
            metrics_instance = {
                "time": float(get_value_by_pattern(row, 'time', None)),
                "throughput": float(get_value_by_pattern(row, 'throughput', None))
            }
            latency = {}
            for key, pattern in LATENCY_ATTRIBUTE_MAPPING:
                value = get_value_by_pattern(row, pattern, None)
                latency[key] = float("{:.4}".format(value)) if value else value
            metrics_instance['latency'] = latency
            incremental_metrics.append(metrics_instance)
        return incremental_metrics
