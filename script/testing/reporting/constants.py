UNKNOWN_RESULT = 'unknown'
LATENCY_ATTRIBUTE_MAPPING = [
    # key = key in publish result json, value= string to search OLTPBench results for
    # TODO(WAN): this mapping could probably be a.. map? {}?
    ('l_25', '25'), ('l_75', '75'), ('l_90', '90'), ('l_95', '95'), ('l_99', '99'),
    ('avg', 'av'), ('median', 'median'), ('min', 'min'), ('max', 'max')]
