#!/usr/bin/env python3
"""
Main script for workload forecasting.
Example usage:
- Generate data (runs OLTP benchmark on the built database) and perform training, and save the trained model
    ./forecaster --gen_data --model_save_path=model.pickle

- Use the trained models to generate predictions.
    ./forecaster --model_load_path=model.pickle --test_file=test_query.csv


TODO:
    - Better metrics for training and prediction (currently not focusing on models' accuracy yet)
    - Multiple models (currently only simple-one-layer-untuned LSTM used)
    - API and interaction with Pilot
"""

import sys
from pathlib import Path
sys.path.insert(0, str((Path.cwd() / '..' / 'testing').absolute()))

import argparse
import pickle
import numpy as np
from functools import cached_property
from typing import List, Tuple
from util.constants import LOG
from self_driving.forecast import gen_oltp_trace
from self_driving.constants import (
    DEFAULT_TPCC_WEIGHTS,
    DEFAULT_QUERY_TRACE_FILE,
    DEFAULT_WORKLOAD_PATTERN,
    DEFAULT_ITER_NUM)
from models import LSTM, ForecastModel
from cluster import QueryCluster
from data_loader import DataLoader


# Interval duration for aggregation in microseconds
INTERVAL_MICRO_SEC = 500000

# Number of Microseconds per second
MICRO_SEC_PER_SEC = 1000000

# Number of data points in a sequence
SEQ_LEN = 10 * MICRO_SEC_PER_SEC // INTERVAL_MICRO_SEC

# Number of data points for the horizon
HORIZON_LEN = 30 * MICRO_SEC_PER_SEC // INTERVAL_MICRO_SEC

# Number of data points for testing set
EVAL_DATA_SIZE = 2 * SEQ_LEN + HORIZON_LEN

argp = argparse.ArgumentParser(description="Query Load Forecaster")

# Generation stage related options
argp.add_argument(
    "--gen_data",
    default=False,
    action="store_true",
    help="If specified, OLTP benchmark would be downloaded and built to generate the query trace data")
argp.add_argument(
    "--tpcc_weight",
    type=str,
    default=DEFAULT_TPCC_WEIGHTS,
    help="Workload weights for the TPCC")
argp.add_argument(
    "--tpcc_rates",
    nargs="+",
    default=DEFAULT_WORKLOAD_PATTERN,
    help="Rate array for the TPCC workload")
argp.add_argument(
    "--pattern_iter",
    type=int,
    default=DEFAULT_ITER_NUM,
    help="Number of iterations the DEFAULT_WORKLOAD_PATTERN should be run")
argp.add_argument("--trace_file", default=DEFAULT_QUERY_TRACE_FILE,
                  help="Path to the query trace file", metavar="FILE")

# Model specific
argp.add_argument("--seq_len", type=int, default=SEQ_LEN,
                  help="Length of one sequence in number of data points")
argp.add_argument(
    "--horizon_len",
    type=int,
    default=HORIZON_LEN,
    help="Length of the horizon in number of data points, "
         "aka, how many further in the a sequence is used for prediction"
)

# Training stage related options
argp.add_argument("--model_save_path", metavar="FILE",
                  help="Where the model trained will be stored")
argp.add_argument(
    "--eval_size",
    type=int,
    default=EVAL_DATA_SIZE,
    help="Length of the evaluation data set length in number of data points")
argp.add_argument("--lr", type=float, default=0.001, help="Learning rate")
argp.add_argument("--epochs", type=int, default=10,
                  help="Number of epochs for training")

# Testing stage related options
argp.add_argument(
    "--model_load_path",
    default="model.pickle",
    metavar="FILE",
    help="Where the model should be loaded from")
argp.add_argument(
    "--test_file",
    help="Path to the test query trace file",
    metavar="FILE")


class Forecaster:
    """
    A wrapper around various ForecastModels, that prepares training and evaluation data.
    """

    def __init__(
            self,
            data: np.ndarray,
            test_mode: bool = False,
            eval_size: int = EVAL_DATA_SIZE,
            seq_len: int = SEQ_LEN,
            horizon_len: int = HORIZON_LEN) -> None:
        """
        :param data:  1D time-series data for a query cluster
        :param test_mode: True If the Loader is for testing
        :param eval_size: Number of data points used for evaluation(testing)
        :param seq_len: Length of a sequence
        :param horizon_len: Horizon length
        """
        self.seq_len = seq_len
        self.horizon_len = horizon_len
        self.test_mode = test_mode
        self.eval_data_size = eval_size

        self.train_raw_data, self.test_raw_data = self._split_data(data)

    def _split_data(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Split the raw data into a training set, and a testing(evaluation) set.
        :param data: All the raw data
        :return: traing, test raw data set
        """
        if self.test_mode:
            self.test_set_size = len(data)
        else:
            self.test_set_size = self.eval_data_size

        split_idx = len(data) - self.test_set_size
        # First part as the training set
        train_raw_data = data[:split_idx]

        # Last part as the testing set
        test_raw_data = data[split_idx:]

        return train_raw_data, test_raw_data

    @cached_property
    def train_seqs(self) -> List[Tuple[np.ndarray, np.ndarray]]:
        """
        Create training sequences.  Each training sample consists of:
         - A list of data points as input
         - A label (target) value in some horizon
        :return: training sequences
        """
        seq_len = self.seq_len
        horizon = self.horizon_len
        input_data = self.train_raw_data

        seqs = []
        for i in range(len(input_data) - seq_len - horizon):
            seq = input_data[i:i + seq_len].reshape(-1, 1)

            # Look beyond the horizon to get the label
            label_i = i + seq_len + horizon
            label = input_data[label_i: label_i + 1].reshape(1, -1)

            seqs.append((seq, label))
        return seqs

    @cached_property
    def test_seqs(self) -> List[np.ndarray]:
        """
        Generate a list of test sequence for evaluation
        :return:  Test sequences for evaluation
        """
        input_data = self.test_raw_data

        seqs = []
        for i in range(len(input_data) - self.seq_len):
            seq = input_data[i:i + self.seq_len].reshape(-1, 1)
            seqs.append(seq)

        return seqs

    def train(self, models: List[ForecastModel]) -> None:
        """
        :param models:  A list of ForecastModels to fit
        :return:
        """
        train_seqs = self.train_seqs
        for model in models:
            model.fit(train_seqs)

    def eval(self, model: ForecastModel) -> np.ndarray:
        """
        Evaluate a model on the test dataset
        :param model: Model to evaluate
        :return: Test results
        """
        test_data = self.test_seqs

        results = []
        for seq in test_data:
            pred = model.predict(seq)
            np.append(results, pred)

        return results


if __name__ == "__main__":
    args = argp.parse_args()

    if args.test_file is None:

        # Generate OLTP trace file
        if args.gen_data:
            gen_oltp_trace(
                tpcc_weight=args.tpcc_weight,
                tpcc_rates=args.tpcc_rates,
                pattern_iter=args.pattern_iter)

            trace_file = DEFAULT_QUERY_TRACE_FILE
        else:
            trace_file = args.trace_file

        data_loader = DataLoader(
            query_trace_file=trace_file,
            interval_us=INTERVAL_MICRO_SEC)
        x_transformer, y_transformer = data_loader.get_transformers()

        # FIXME: Assuming all the queries in the current trace file are from
        # the same cluster for now
        cluster_trace = QueryCluster(data_loader.get_ts_data())

        # Aggregated time-series from the cluster
        train_timeseries = cluster_trace.get_timeseries()

        # Perform training on the trace file
        lstm_model = LSTM(
            lr=args.lr,
            epochs=args.epochs,
            x_transformer=x_transformer,
            y_transformer=y_transformer)
        trainer = Forecaster(
            train_timeseries,
            seq_len=args.seq_len,
            eval_size=args.eval_size,
            horizon_len=args.horizon_len)
        trainer.train([lstm_model])

        # Save the model
        if args.model_save_path:
            with open(args.model_save_path, "wb") as f:
                pickle.dump(lstm_model, f)
    else:
        # Do inference on a trained model
        data_loader = DataLoader(
            query_trace_file=args.test_file,
            interval_us=INTERVAL_MICRO_SEC)

        with open(args.model_load_path, "rb") as f:
            model = pickle.load(f)

        # FIXME: Assuming all the queries in the current trace file are from
        # the same cluster for now
        timeseries = data_loader.get_ts_data()
        cluster_trace = QueryCluster(timeseries)

        # Aggregated time-series from the cluster
        test_timeseries = cluster_trace.get_timeseries()

        pred = model.predict(test_timeseries[:args.seq_len].reshape(1, -1))

        query_pred = cluster_trace.segregate([pred])
        for qid, ts in query_pred.items():
            LOG.info(
                f"[Query: {qid} @idx={args.seq_len+args.horizon_len}] "
                f"actual={timeseries[qid][args.seq_len+args.horizon_len]},pred={ts[0]:.1f}")
