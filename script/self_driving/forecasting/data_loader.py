#!/usr/bin/env python3
"""
This file contains data loading logic from the query trace file produced. Hardcoded CSV format needs to be synced with
query trace producer.
"""

import logging
import csv
from typing import Dict
import numpy as np


class DataLoader:
    # Hardcoded db_oid column index in the query_trace file
    DBOID_IDX = 0
    # Hardcoded query_id column index in the query_trace file
    QID_IDX = 1
    # Hardcoded timestamp column index in the query_trace file
    TS_IDX = 2

    def __init__(self,
                 interval_us: int,
                 query_trace_file: str,
                 ) -> None:
        """
        A Dataloader represents a query trace file. The format of the CSV is hardcoded as class attributes, e.g QID_IDX
        The loader transforms the timestamps in the original file into time-series for each query id.
        :param interval_us: Interval for the time-series
        :param query_trace_file: Query trace CSV file
        """
        self._query_trace_file = query_trace_file
        self._interval_us = interval_us

        data = self._load_data()
        self._to_timeseries(data)

    def _load_data(self) -> np.ndarray:
        """
        Load data from csv
        :return: Loaded 2D numpy array of [db_oid, query_id, timestamp]
        """
        logging.info(f"Loading data from {self._query_trace_file}")
        # Load data from the files
        with open(self._query_trace_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = np.array(
                [[int(r['db_oid']), int(r[' query_id']), int(r[' timestamp'])] for r in reader])

            if len(data) == 0:
                raise ValueError("Empty trace file")

            return data

    def _to_timeseries(self, data: np.ndarray) -> None:
        """
        Convert the 2D array with query id and timestamps into a map of time-series for each query id
        :param data: Loaded 2D numpy array of [query_id, timestamp]
        :return: None
        """
        # Query trace file is sorted by timestamps
        start_timestamp = data[0][self.TS_IDX]
        end_timestamp = data[-1][self.TS_IDX]

        if end_timestamp - start_timestamp <= 1:
            raise ValueError(
                "Empty data set with start timestamp >= end timestamp.")

        # Number of data points in the new time-series
        num_buckets = (end_timestamp - start_timestamp -
                       1) // self._interval_us + 1

        # Iterate through the timestamps
        self._ts_data = {}
        for i in range(len(data)):
            t = data[i][self.TS_IDX]
            qid = data[i][self.QID_IDX]

            # Initialize a new query's time-series
            if self._ts_data.get(qid) is None:
                self._ts_data[qid] = np.zeros(num_buckets)

            # Bucket index
            bi = (t - start_timestamp) // self._interval_us
            self._ts_data[qid][bi] += 1

    def get_ts_data(self) -> Dict:
        return self._ts_data
