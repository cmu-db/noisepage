#!/usr/bin/env python3

from collections import namedtuple

MemoryInfo = namedtuple("MemoryInfo", ["rss", "vms"])


class MemoryMetrics:
    def __init__(self):
        self.mem_info_dict = {}

    def get_avg(self):
        """
        Obtain the average value of all the memory info data
        """
        # The average cannot be calculated since there is no mem info data
        if not self.mem_info_dict:
            return MemoryInfo(None, None)

        total_cnt = len(self.mem_info_dict)
        total_rss = total_vms = 0
        for rss, vms in self.mem_info_dict.values():
            total_rss += rss
            total_vms += vms
        avg_rss = total_rss / total_cnt
        avg_vms = total_vms / total_cnt
        return MemoryInfo(avg_rss, avg_vms)