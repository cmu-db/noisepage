import csv
import json

from oltpbench.reporting.utils import get_value_by_pattern
from oltpbench.reporting.constants import LATENCY_ATTRIBUTE_MAPPING

def parse_res_file(path):
    """Read data from file ends with ".res".

    Args:
        path (str): The position of the res file.

    Returns:
        incremental_metrics (list, json array): The throughput at different time.

    """
    # time, throughput, min_lat, lat_25th, median_lat, avg_lat, lat_75th, lat_90th, lat_95th, lat_99th, max_lat = [
    # ], [], [], [], [], [], [], [], [], [], []
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        incremental_metrics = []
        for row in reader:
            metrics_instance = {
                "time": float(get_value_by_pattern(row,'time',None)),
                "throughput": float(get_value_by_pattern(row, 'throughput', None))
            }
            # time.append(float(row['time(sec)']))
            # throughput.append(float(row[' throughput(req/sec)']))
            latency = {}
            for key, pattern in LATENCY_ATTRIBUTE_MAPPING:
                value = get_value_by_pattern(row, pattern, None)
                latency[key] = float(value) if value else value
            metrics_instance['latency'] = latency
            incremental_metrics.append(metrics_instance)
            # min_lat.append(float(row[' min_lat(ms)']))
            # lat_25th.append(float(row[' 25th_lat(ms)']))
            # median_lat.append(float(row[' median_lat(ms)']))
            # avg_lat.append(float(row[' avg_lat(ms)']))
            # lat_75th.append(float(row[' 75th_lat(ms)']))
            # lat_90th.append(float(row[' 90th_lat(ms)']))
            # lat_95th.append(float(row[' 95th_lat(ms)']))
            # lat_99th.append(float(row[' 99th_lat(ms)']))
            # max_lat.append(float(row[' max_lat(ms)']))
        # incremental_metrics = [{"time": t, "throughput": tp, "latency":latency}
        #                        for t, tp, ml, l25, mel, al, l75, l90, l95, l99, mal in zip(time, throughput, min_lat, lat_25th, median_lat, avg_lat, lat_75th, lat_90th, lat_95th, lat_99th, max_lat)]
        return incremental_metrics
