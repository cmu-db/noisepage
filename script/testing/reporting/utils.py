#!/usr/bin/python3


def get_value_by_pattern(dict_obj, pattern, default):
    """
    This is similar to .get() for a dict but it matches the
    key based on a substring. This function is case insensitive.
    """
    for key, value in dict_obj.items():
        if pattern.lower() in key.lower():
            return value
    return default


def add_mem_metrics(metrics, mem_metrics):
    """
    Add the memory metrics to the overal reporting metrics
    """
    add_overall_mem_metrics(metrics, mem_metrics)
    add_incremental_mem_metrics(metrics, mem_metrics)


def add_overall_mem_metrics(metrics, mem_metrics):
    """
    Add the average memory metrics to the overal reporting metrics
    """
    avg_mem_info = mem_metrics.get_avg()
    metrics['memory_info'] = {
        'rss': {
            'avg': float("{:.4}".format(avg_mem_info.rss)),
        },
        'vms': {
            'avg': float("{:.4}".format(avg_mem_info.vms)),
        },
    }


def add_incremental_mem_metrics(metrics, mem_metrics):
    """
    Add the memory info to the incremental metrics by time
    """
    incremental_metrics = metrics.get('incremental_metrics', [])
    for metrics in incremental_metrics:
        mem_info = mem_metrics.mem_info_dict.get(metrics.get('time'))
        if mem_info:
            metrics['memory_info'] = {
                'rss': mem_info.rss,
                'vms': mem_info.vms,
            }