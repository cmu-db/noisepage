#!/usr/bin/env python3
"""
TODO(ricky):
The modeling scripts should be under a project, with __init__
What should be the best dependency between the modeling scripts and testing package?
"""
import sys
from pathlib import Path
sys.path.insert(0, str((Path.cwd() / '..' / 'testing').absolute()))

from oltpbench.test_case_oltp import TestCaseOLTPBench
from oltpbench.run_oltpbench import TestOLTPBench
from oltpbench.constants import OLTPBENCH_GIT_LOCAL_PATH
from util.constants import LOG, ErrorCode
from util.common import run_command
from util.db_server import NoisePageServer
from xml.etree import ElementTree
from typing import Dict, List, Optional, Tuple
from functools import cached_property
from abc import ABC, abstractmethod
import torch.nn as nn
import torch
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import matplotlib.pyplot as plt
import pickle
import logging
import csv
import argparse


# Default pattern: High -> Low  ...
DEFAULT_TPCC_RATE = 10000
DEFAULT_WORKLOAD_PATTERN = [DEFAULT_TPCC_RATE, DEFAULT_TPCC_RATE // 10]

# Default time, runs 30 second for a work phase
DEFAULT_TPCC_TIME_SEC = 30

# Run the workload pattern for 30 iterations
DEFAULT_ITER_NUM = 1

# Load the workload pattern - based on the tpcc.json in
# testing/oltpbench/config
DEFAULT_TPCC_WEIGHTS = "45,43,4,4,4"
DEFAULT_OLTP_TEST_CASE = {
    "benchmark": "tpcc",
    "query_mode": "extended",
    "terminals": 4,
    "scale_factor": 4,
    "weights": DEFAULT_TPCC_WEIGHTS
}

# Enable query trace collection, it will produce a query_trace.csv at CWD
DEFAULT_OLTP_SERVER_ARGS = {
    "server_args": {
        'query_trace_metrics_enable': None
    }
}

# Default query_trace file name
DEFAULT_QUERY_TRACE_FILE = "query_trace.csv"

# Interval duration for aggregation in microseconds
INTERVAL_MICRO_SEC = 500000

# Number of Microseconds per second
MICRO_SEC_PER_SEC = 1000000

# First 35 seconds are for loading
LOAD_PHASE_NUM_DATA = int(MICRO_SEC_PER_SEC / INTERVAL_MICRO_SEC * 35)

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


def config_forecast_data(xml_config_file: str, rate_pattern: List[int]) -> None:
    """
     Modify a OLTP config file to follow a certain pattern in its duration.

    :param xml_config_file:
    :param rate_pattern:
    :return:
    """
    xml = ElementTree.parse(xml_config_file)
    root = xml.getroot()
    works = root.find("works")
    works.clear()

    # Set the work pattern
    for rate in rate_pattern:
        work = ElementTree.Element("work")

        # NOTE: rate has to be before weights... This is what the OLTP expects
        elems = [
            ("time", str(DEFAULT_TPCC_TIME_SEC)),
            ("rate", str(rate)),
            ("weights", DEFAULT_TPCC_WEIGHTS)
        ]

        for name, text in elems:
            elem = ElementTree.Element(name)
            elem.text = text
            work.append(elem)

        works.append(work)

    # Write back result
    xml.write(xml_config_file)


def gen_oltp_trace(
        tpcc_weight: str, tpcc_rates: List[int], pattern_iter: int) -> bool:
    """
    Generates the trace by running OLTP TPCC benchmark on the built database
    :param tpcc_weight:  Weight for the TPCC workload
    :param tpcc_rates: Arrival rates for each phase in a pattern
    :param pattern_iter:  Number of patterns
    :return: True when data generation succeeds
    """
    # Remove the old query_trace/query_text.csv
    Path(DEFAULT_QUERY_TRACE_FILE).unlink(missing_ok=True)

    # Server is running when this returns
    oltp_server = TestOLTPBench(DEFAULT_OLTP_SERVER_ARGS)
    db_server = oltp_server.db_instance
    db_server.run_db()

    # Download the OLTP repo and build it
    oltp_server.run_pre_suite()

    # Load the workload pattern - based on the tpcc.json in
    # testing/oltpbench/config
    test_case_config = DEFAULT_OLTP_TEST_CASE
    test_case_config["weights"] = tpcc_weight
    test_case = TestCaseOLTPBench(test_case_config)

    # Prep the test case build the result dir
    test_case.run_pre_test()

    rates = tpcc_rates * pattern_iter
    config_forecast_data(test_case.xml_config, rates)

    # Run the actual test
    ret_val, _, stderr = run_command(test_case.test_command,
                                     test_case.test_error_msg,
                                     cwd=test_case.test_command_cwd)
    if ret_val != ErrorCode.SUCCESS:
        LOG.error(stderr)
        return False

    # Clean up, disconnect the DB
    db_server.stop_db()
    db_server.delete_wal()

    if not Path(DEFAULT_QUERY_TRACE_FILE).exists():
        LOG.error(
            f"Missing {DEFAULT_QUERY_TRACE_FILE} at CWD after running OLTP TPCC")
        return False

    return True


class QueryCluster:
    """
    Represents query traces from a single cluster. For queries in the same cluster, they will be aggregated
    into a single time-series to be used as training input for training. The time-series predicted by the model will
    then be converted to different query traces for a query cluster
    """

    def __init__(self, traces: Dict):
        """
        NOTE(ricky): I believe this per-cluster query representation will change once we have clustering component added.
        For now, it simply takes a map of time-series data for each query id.

        :param traces: Map of (id -> timeseries) for each query id
        """
        self.traces = traces
        self._aggregate()

    def _aggregate(self) -> None:
        """
        Aggregate time-series of multiple queries in the same cluster into one time-series
        It stores the aggregated times-eries at self.timeseries, and the ratio map of queries in the
        same cluster at self.ratio_map
        """
        cnt_map = {}
        total_cnt = 0
        all_series = []
        for qid, timeseries in self.traces.items():
            cnt = sum(timeseries)
            all_series.append(timeseries)
            total_cnt += cnt

            cnt_map[qid] = cnt

        # Sum all timeseries element-wise
        self.timeseries = np.array([sum(x) for x in zip(*all_series)])

        # Compute distribution of each query id in the cluster
        self.ratio_map = {}
        for qid, cnt in cnt_map.items():
            self.ratio_map[qid] = cnt / total_cnt


    def get_timeseries(self) -> np.ndarray:
        """
        Get the aggregate time-series for this cluster
        :return: Time-series for the cluster
        """
        return self.timeseries

    def segregate(self, timeseries: List[float]) -> Dict:
        """
        From an aggregated time-series, segregate it into multiple time-series, one for each query in the cluster.
        :param timeseries: Aggregated time-series
        :return: Time-series for each query id, dict{query id: time-series}
        """
        result = {}
        for qid, ratio in self.ratio_map.items():
            result[qid] = list([x * ratio for x in timeseries])
        return result


class DataLoader:
    # Hardcoded query_id column index in the query_trace file
    QID_IDX = 0
    # Hardcoded timestamp column index in the query_trace file
    TS_IDX = 1

    def __init__(self,
                 interval_us: int ,
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
            raise ValueError("Empty data set with start timestamp >= end timestamp.")

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


class ForecastModel(ABC):
    """
    Interface for all the forecasting models
    """
    def __init__(self):
        pass

    @abstractmethod
    def fit(self, train_seqs: List[Tuple[np.ndarray, np.ndarray]]) -> None:
        """
        Fit the model with sequences
        :param train_seqs: List of training sequences and the expected output label in a certain horizon
        :return:
        """
        pass

    @abstractmethod
    def test(self, test_seq: np.ndarray) -> float:
        """
        Test a fitted model with a sequence.
        :param test_seq:  1D Test sequence
        :return: Predicted value at certain horizon
        """
        pass


class ForecastTrainer:
    """
    A wrapper around various ForecastModels, that prepares training and evaluation data.
    """
    def __init__(self, data: np.ndarray, test_mode: bool = False, eval_size: int = EVAL_DATA_SIZE, seq_len : int = SEQ_LEN, horizon_len: int = HORIZON_LEN) -> None:

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
            seq = input_data[i:i + seq_len]

            # Look beyond the horizon to get the label
            label_i = i + seq_len + horizon
            label = input_data[label_i: label_i + 1]

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
        for i in range(len(input_data)-self.seq_len):
            seq = input_data[i:i + self.seq_len]
            if self.x_transformer:
                seq = self.x_transformer.transform(seq)
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
            pred = model.test(seq)
            np.append(results, pred)

        return results


class LSTM(nn.Module, ForecastModel):
    """
    A simple LSTM model
    """

    def __init__(self, input_size:int =1, hidden_layer_size:int =100, output_size:int =1, lr:float = 0.001, epochs:int=10, normalize:bool=True,
                 x_transformer=None, y_transformer=None):
        """
        :param input_size: One data point that is fed into the LSTM each time
        :param hidden_layer_size:
        :param output_size: One output data point
        :param lr: learning rate while fitting
        :param epochs: number of epochs for fitting
        :param x_transformer: To transform the seq input
        :param y_transformer: To transform the label
        """
        super().__init__()
        self.hidden_layer_size = hidden_layer_size

        self.lstm = nn.LSTM(input_size, hidden_layer_size)

        self.linear = nn.Linear(hidden_layer_size, output_size)

        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                            torch.zeros(1, 1, self.hidden_layer_size))

        self.epochs = epochs
        self.lr = lr

        self.x_transformer = x_transformer
        self.y_transformer = y_transformer

    def forward(self, input_seq: torch.FloatTensor) -> float:
        """
        Forward propogation
        :param input_seq:  1D FloatTensor
        :return: A single value prediction
        """
        lstm_out, self.hidden_cell = self.lstm(
            input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(len(input_seq), -1))
        return predictions[-1]

    def fit(self, train_seqs: List[Tuple[np.ndarray, np.ndarray]]) -> None:
        """
        Perform training on the time series trace data.

        :param data_loader: Dataloader
        :param lr: learing rate
        :param epochs: number of epochs
        """
        epochs = self.epochs
        lr = self.lr

        # Training specifics
        loss_function = nn.MSELoss()
        optimizer = torch.optim.Adam(self.parameters(), lr=lr)
        LOG.info(f"Training with {len(train_seqs)} samples, {epochs} epochs:")
        for i in range(epochs):
            for seq, labels in train_seqs:
                optimizer.zero_grad()

                self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                                    torch.zeros(1, 1, self.hidden_layer_size))
                if self.x_transformer:
                    # Reshape to 2D ndarray required by MinMaxScaler API
                    seq = self.x_transformer.transform(seq.reshape(-1, 1))
                    labels = self.y_transformer.transform(labels.reshape(1, -1))

                seq = torch.FloatTensor(seq).view(-1)
                labels = torch.FloatTensor(labels).view(-1)

                y_pred = self(seq)

                single_loss = loss_function(y_pred, labels)
                single_loss.backward()
                optimizer.step()

            if i % 25 == 0:
                LOG.info(f'[LSTM FIT]epoch: {i+1:3} loss: {single_loss.item():10.8f}')

        LOG.info(f'[LSTM FIT]epoch: {epochs:3} loss: {single_loss.item():10.10f}')

    def test(self, seq: np.ndarray) -> float:
        """
        Perform inference on a dataset. Returns a list of prediction results
        :param data_loader: DataLoader for the test input
        :return: Prediction results
        """
        # To tensor
        if self.x_transformer:
            seq = self.x_transformer.transform(seq.reshape(-1, 1))
        seq = torch.FloatTensor(seq).view(-1)

        with torch.no_grad():
            model.hidden = (torch.zeros(1, 1, model.hidden_layer_size),
                            torch.zeros(1, 1, model.hidden_layer_size))
            pred = model(seq)

        if self.y_transformer:
            pred = self.y_transformer.inverse_transform(pred.reshape(1, -1))

        return pred.item()


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

        data_loader = DataLoader(query_trace_file=trace_file, interval_us=INTERVAL_MICRO_SEC)
        x_transformer, y_transformer = data_loader.get_transformers()

        # FIXME: Assuming all the queries in the current trace file are from the same cluster for now
        cluster_trace = QueryCluster(data_loader.get_ts_data())

        # Aggregated time-series from the cluster
        train_timeseries = cluster_trace.get_timeseries()

        # Perform training on the trace file
        lstm_model = LSTM(lr=args.lr, epochs=args.epochs, x_transformer=x_transformer, y_transformer=y_transformer)
        trainer = ForecastTrainer(train_timeseries, seq_len=args.seq_len, eval_size=args.eval_size, horizon_len=args.horizon_len)
        trainer.train([lstm_model])

        # Save the model
        if args.model_save_path:
            with open(args.model_save_path, "wb") as f:
                pickle.dump(lstm_model, f)
    else:
        # Do inference on a trained model
        data_loader = DataLoader(query_trace_file=args.test_file, interval_us=INTERVAL_MICRO_SEC)

        with open(args.model_load_path, "rb") as f:
            model = pickle.load(f)
        x_transformer, y_transformer = data_loader.get_transformers()
        model.x_transformer = x_transformer
        model.y_transformer = y_transformer

        # FIXME: Assuming all the queries in the current trace file are from the same cluster for now
        timeseries = data_loader.get_ts_data()
        cluster_trace = QueryCluster(timeseries)

        # Aggregated time-series from the cluster
        test_timeseries = cluster_trace.get_timeseries()

        pred = model.test(test_timeseries[:args.seq_len])

        query_pred = cluster_trace.segregate([pred])
        for qid, ts in query_pred.items():
            LOG.info(f"[Query: {qid} @idx={args.seq_len+args.horizon_len}] "
                     f"actual={timeseries[qid][args.seq_len+args.horizon_len]},pred={ts[0]:.1f}")
