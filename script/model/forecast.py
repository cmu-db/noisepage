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

# Interval duration for aggregation in MS
INTERVAL_MS = 500000

# Number of MS per second
MS_PER_SEC = 1000000

# First 35 seconds are for loading
LOAD_PHASE_NUM_DATA = int(MS_PER_SEC / INTERVAL_MS * 35)

# Number of data points in a sequence
SEQ_LEN = 10 * MS_PER_SEC // INTERVAL_MS

# Number of data points for the horizon
HORIZON_LEN = 30 * MS_PER_SEC // INTERVAL_MS

# Number of data points for testing set
EVAL_DATA_SIZE = 2 * SEQ_LEN + HORIZON_LEN


def plot_ts(series: List[Tuple], interval_sec: float) -> None:
    """
    Utility function for plotting
    :param series: List of tuple of (Label:str, X:List[int], Data:List[Numerical]):
        e.g. [("Train", x_labels, data)]
    :param interval_sec: Interval seconds (used only in descriptive manner)
    :return:
    """
    plt.title(f'Time series Query Count per {interval_sec} seconds')
    plt.ylabel(f'Query Count / {interval_sec}')
    plt.xlabel('Time')
    plt.grid(True)
    plt.autoscale(axis='x', tight=True)

    for serie in series:
        label, x, data = serie
        plt.plot(x, data, label=label)
    plt.legend()


def gen_forecast_data(xml_config_file: str, rate_pattern: List[int]) -> None:
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
    gen_forecast_data(test_case.xml_config, rates)

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


class DataLoader:
    # Hardcoded timestamp column index in the query_trace file
    TS_IDX = 1

    def __init__(
            self,
            query_trace_file: str = DEFAULT_QUERY_TRACE_FILE,
            test: bool = False,
            eval_size: int = EVAL_DATA_SIZE,
            seq_len: int = SEQ_LEN,
            horizon_len: int = HORIZON_LEN) -> None:
        """
        :param query_trace_file: Where to process the query trace
        :param test: If the Loader is for testing
        """
        self.query_trace_file = query_trace_file

        self.seq_len = seq_len
        self.horizon_len = horizon_len
        self.scaler = MinMaxScaler(feature_range=(-1, 1))
        self.test_mode = test
        self.eval_data_size = eval_size

        self._load_data()

    def _load_data(self) -> None:
        LOG.info(f"Loading data from {self.query_trace_file}")
        # Load data from the files
        with open(self.query_trace_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = np.array(
                [[int(r['query_id']), int(r[' timestamp'])] for r in reader])

            data = self._aggregate_data(data, INTERVAL_MS)
            self.train_raw_data, self.test_raw_data = self._split_data(data)
            LOG.info(
                f"Loaded data successfully: num_test={len(self.test_raw_data)}, num_train={len(self.train_raw_data)}, "
                f"seq_len={self.seq_len}, horizon={self.horizon_len}")

    def _normalize_data(self, data: np.ndarray) -> np.ndarray:
        norm_data = self.scaler.fit_transform(data.reshape(-1, 1))
        return norm_data

    def _aggregate_data(self, data: np.ndarray,
                        interval_ms: int) -> Optional[np.ndarray]:
        """
        Aggregate multiple time-series data points into one bucket to generate a new time-series
        data set.
        It assumes the original time-series data has a Timestamp value, and the generated time-series
        will be the number of data points fall into an interval for each interval in the original dataset

        :param data: Time-series data with TIMESTAMP column
        :param interval_ms: Interval duratoin in MS
        :return:
        """
        start_t = data[0][self.TS_IDX]
        end_t = data[-1][self.TS_IDX]

        if end_t - start_t <= 1:
            return None

        # Number of data points in the new time-series
        num_buckets = (end_t - start_t - 1) // interval_ms + 1

        new_data = np.zeros(num_buckets)

        for i in range(len(data)):
            t = data[i][self.TS_IDX]
            bi = (t - start_t) // interval_ms

            new_data[bi] += 1

        new_data = new_data[LOAD_PHASE_NUM_DATA:]
        return new_data

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

    def create_seqs(self) -> List[Tuple[torch.Tensor, torch.Tensor]]:
        """
        Create training sequences.  Each training sample consists of:
         - A list of data points as input
         - A label (target) value in some horizon
        :return: training sequences
        """
        seq_len = self.seq_len
        horizon = self.horizon_len
        train_norm_data = self._normalize_data(self.train_raw_data)

        input_data = torch.FloatTensor(train_norm_data).view(-1)
        seqs = []
        for i in range(len(input_data) - seq_len - horizon):
            seq = input_data[i:i + seq_len]

            # Look beyond the horizon to get the label
            label_i = i + seq_len + horizon
            label = input_data[label_i: label_i + 1]

            seqs.append((seq, label))

        return seqs

    def get_test_input(self) -> List[float]:
        """
        Get the test input (normalized)
        :return: test time-series data
        """
        test_input_norm = self._normalize_data(self.test_raw_data)
        test_input_list = test_input_norm.tolist()
        return test_input_list

    def stat_util(self) -> None:
        trace_start_ms = self.data[0][1]
        trace_end_ms = self.data[-1][1]
        trace_nsample = len(self.data)

        print(
            f"{trace_nsample} samples from {trace_start_ms} to {trace_end_ms}\n"
            f"{(trace_end_ms - trace_start_ms) / 1000000}seconds")


class LSTM(nn.Module):
    """
    A simple LSTM that serves as a placeholder for the training model
    """

    def __init__(self, input_size=1, hidden_layer_size=100, output_size=1):
        """
        :param input_size: One data point that is fed into the LSTM each time
        :param hidden_layer_size:
        :param output_size: One output data point
        """
        super().__init__()
        self.hidden_layer_size = hidden_layer_size

        self.lstm = nn.LSTM(input_size, hidden_layer_size)

        self.linear = nn.Linear(hidden_layer_size, output_size)

        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                            torch.zeros(1, 1, self.hidden_layer_size))

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

    def do_train(self, data_loader: DataLoader,
                 lr: float = 0.0001, epochs: int = 10) -> None:
        """
        Perform training on the time series trace data.

        :param data_loader: Dataloader
        :param lr: learing rate
        :param epochs: number of epochs
        """
        train_seqs = data_loader.create_seqs()

        # Training specifics
        loss_function = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        LOG.info(f"Training with {len(train_seqs)} samples, {epochs} epochs:")
        for i in range(epochs):
            for seq, labels in train_seqs:
                optimizer.zero_grad()

                self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                                    torch.zeros(1, 1, self.hidden_layer_size))

                y_pred = self(seq)

                single_loss = loss_function(y_pred, labels)
                single_loss.backward()
                optimizer.step()

            if i % 25 == 0:
                LOG.info(f'epoch: {i+1:3} loss: {single_loss.item():10.8f}')

        LOG.info(f'epoch: {epochs:3} loss: {single_loss.item():10.10f}')

    def do_test(self, data_loader: DataLoader) -> List[float]:
        """
        Perform inference on a dataset. Returns a list of prediction results
        :param data_loader: DataLoader for the test input
        :return: Prediction results
        """
        self.eval()
        test_input_list = data_loader.get_test_input()
        test_outputs = []
        for i in range(len(test_input_list)):
            # To tensor
            seq = torch.FloatTensor(test_input_list[i:i + data_loader.seq_len])
            with torch.no_grad():
                model.hidden = (torch.zeros(1, 1, model.hidden_layer_size),
                                torch.zeros(1, 1, model.hidden_layer_size))
                test_outputs.append(model(seq).item())

        # Inverse Normalize the data
        test_outputs = data_loader.scaler.inverse_transform(
            np.array(test_outputs).reshape(-1, 1))
        return test_outputs


if __name__ == "__main__":
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

    args = argp.parse_args()

    if args.test_file is None:

        # Generate OLTP trace file
        if args.gen_data:
            gen_oltp_trace(
                tpcc_weight=args.tpcc_weight,
                tpcc_rates=args.tpcc_rates,
                pattern_iter=args.pattern_iter)
            data_loader = DataLoader(
                DEFAULT_QUERY_TRACE_FILE,
                eval_size=args.eval_size,
                seq_len=args.seq_len,
                horizon_len=args.horizon_len)
        else:
            data_loader = DataLoader(
                args.trace_file,
                eval_size=args.eval_size,
                seq_len=args.seq_len,
                horizon_len=args.horizon_len)

        # Perform training on the trace file
        model = LSTM()
        model.do_train(data_loader, lr=args.lr, epochs=args.epochs)

        if args.model_save_path:
            torch.save(model.state_dict(), args.model_save_path)
    else:
        # Do inference on a trained model
        model = LSTM()
        model.load_state_dict(torch.load(args.model_load_path))

        data_loader = DataLoader(
            args.test_file,
            test=True,
            eval_size=args.eval_size,
            seq_len=args.seq_len,
            horizon_len=args.horizon_len)
        result = model.do_test(data_loader)

        print("Test Result (first 10):")
        print(result[:10])
        print("...")
