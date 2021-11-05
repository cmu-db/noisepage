import csv

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
                "time": float(gvbp(row, 'time(sec)', None)),
                "throughput": float(gvbp(row, 'throughput(req/sec)', None)),
                "latency": {key: get_latency_val(row, pat)
                            for key, pat in [
                                ('l_25', '25th_lat(ms)'),
                                ('l_75', '75th_lat(ms)'),
                                ('l_90', '90th_lat(ms)'),
                                ('l_95', '95th_lat(ms)'),
                                ('l_99', '99th_lat(ms)'),
                                ('avg', 'avg_lat(ms)'),
                                ('median', 'median_lat(ms)'),
                                ('min', 'min_lat(ms)'),
                                ('max', 'max_lat(ms)')
                            ]}
            })

    return incremental_metrics
