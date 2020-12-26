#!/usr/bin/env python3
"""
This file contains data loading logic from the query trace file produced. Hardcoded CSV format needs to be synced with
query trace producer.
"""

from util.constants import LOG

from typing import Dict, Tuple
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import csv


class DataLoader:
    # Hardcoded query_id column index in the query_trace file
    QID_IDX = 0
    # Hardcoded timestamp column index in the query_trace file
    TS_IDX = 1

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
        self.query_trace_file = query_trace_file
        self.interval_us = interval_us

        data = self._load_data()
        self._to_timeseries(data)

    def _load_data(self) -> np.ndarray:
        """
        Load data from csv
        :return: Loaded 2D numpy array of [query_id, timestamp]
        """
        LOG.info(f"Loading data from {self.query_trace_file}")
        # Load data from the files
        with open(self.query_trace_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = np.array(
                [[int(r['query_id']), int(r[' timestamp'])] for r in reader])

            if len(data) == 0:
                raise ValueError("Empty trace file")

            return data

    def _to_timeseries(self, data: np.ndarray) -> None:
        """
        Convert the 2D array with query id and timestamps into a map of time-series for each query id
        :param data: Laoded 2D numpy array of [query_id, timestamp]
        :return: None
        """
        # Query trace file is sorted by timestamps
        start_t = data[0][self.TS_IDX]
        end_t = data[-1][self.TS_IDX]

        if end_t - start_t <= 1:
            raise ValueError(
                "Empty data set with start timestamp >= end timestamp.")

        # Number of data points in the new time-series
        num_buckets = (end_t - start_t - 1) // self.interval_us + 1

        # Iterate through the timestamps
        self.ts_data = {}
        for i in range(len(data)):
            t = data[i][self.TS_IDX]
            qid = data[i][self.QID_IDX]

            # Initialize a new query's time-series
            if self.ts_data.get(qid) is None:
                self.ts_data[qid] = np.zeros(num_buckets)

            # Bucket index
            bi = (t - start_t) // self.interval_us
            self.ts_data[qid][bi] += 1

    def get_ts_data(self) -> Dict:
        return self.ts_data

    def get_transformers(self) -> Tuple[MinMaxScaler, MinMaxScaler]:
        """
        Get the transformers for the dataset. These should be used by the ForecastModel to normalize the dataset.
        :return: (x transformer, y transformer)
        """
        all_ts = []
        for _, ts in self.ts_data.items():
            all_ts = np.append(all_ts, ts)
        scaler = MinMaxScaler(feature_range=(-1, 1))
        all_ts = all_ts.reshape(-1, 1)
        scaler.fit(all_ts)

        # Time-series data shares the same transformer
        return scaler, scaler
