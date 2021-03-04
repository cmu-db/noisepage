#!/usr/bin/env python3
"""
This file contains cluster related codes for the forecasting query traces. QueryCluster represents query traces of
multiple queries in the same cluster.

TODO: clustering implementation
"""

from typing import Dict, List

import numpy as np


class QueryCluster:
    """
    Represents query traces from a single cluster. For queries in the same cluster, they will be aggregated
    into a single time-series to be used as training input for training. The time-series predicted by the model will
    then be converted to different query traces for a query cluster
    """

    def __init__(self, traces: Dict):
        """
        NOTE(ricky): I believe this per-cluster query representation will change once we have clustering component
        added. For now, it simply takes a map of time-series data for each query id.

        :param traces: Map of (id -> timeseries) for each query id
        """
        self._traces = traces
        self._aggregate()

    def _aggregate(self) -> None:
        """
        Aggregate time-series of multiple queries in the same cluster into one time-series
        It stores the aggregated times-eries at self._timeseries, and the ratio map of queries in the
        same cluster at self._ratio_map
        """
        cnt_map = {}
        total_cnt = 0
        all_series = []
        for qid, timeseries in self._traces.items():
            cnt = sum(timeseries)
            all_series.append(timeseries)
            total_cnt += cnt

            cnt_map[qid] = cnt

        # Sum all timeseries element-wise
        self._timeseries = np.array([sum(x) for x in zip(*all_series)])

        # Compute distribution of each query id in the cluster
        self._ratio_map = {}
        for qid, cnt in cnt_map.items():
            self._ratio_map[qid] = cnt / total_cnt

    def get_timeseries(self) -> np.ndarray:
        """
        Get the aggregate time-series for this cluster
        :return: Time-series for the cluster
        """
        return self._timeseries

    def segregate(self, timeseries: List[float]) -> Dict:
        """
        From an aggregated time-series, segregate it into multiple time-series, one for each query in the cluster.
        :param timeseries: Aggregated time-series
        :return: Time-series for each query id, dict{query id: time-series}
        """
        result = {}
        for qid, ratio in self._ratio_map.items():
            result[qid] = list([x * ratio for x in timeseries])
        return result
