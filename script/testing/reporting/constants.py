UNKNOWN_RESULT = 'unknown'
LATENCY_ATTRIBUTE_MAPPING = [
    # key = key in publish result json, value= string to search OLTPBench results for
    # TODO(WAN): this mapping could probably be a.. map? {}?
    ('l_25', '25th Percentile Latency (microseconds)'),
    ('l_75', '75th Percentile Latency (microseconds)'),
    ('l_90', '90th Percentile Latency (microseconds)'),
    ('l_95', '95th Percentile Latency (microseconds)'),
    ('l_99', '99th Percentile Latency (microseconds)'),
    ('avg', 'Average Latency (microseconds)'),
    ('median', 'Median Latency (microseconds)'),
    ('min', 'Minimum Latency (microseconds)'),
    ('max', 'Maximum Latency (microseconds)')]
