#!/usr/bin/env python3

"""
TODO(ricky):
The modeling scripts should be under a project.
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
from typing import Dict, List, Optional
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

# Load the workload pattern - based on the tpcc.json in testing/oltpbench/config
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


def plot_ts(series: List[Tuple], interval_sec: float) -> None:
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


def gen_oltp_trace() -> bool:
    """
    Generates the trace by running OLTP TPCC benchmark on the built database
    :return:
    """
    # Remove the old query_trace/query_text.csv
    Path("query_text.csv").unlink(missing_ok=True)
    Path("query_trace.csv").unlink(missing_ok=True)

    # Server is running when this returns
    oltp_server = TestOLTPBench(DEFAULT_OLTP_SERVER_ARGS)
    db_server = oltp_server.db_instance
    db_server.run_db()

    # Download the OLTP repo and build it
    oltp_server.run_pre_suite()

    # Load the workload pattern - based on the tpcc.json in testing/oltpbench/config
    test_case = TestCaseOLTPBench(DEFAULT_OLTP_TEST_CASE)

    # Prep the test case build the result dir
    test_case.run_pre_test()

    # Run different rates each for DEFAULT_TPCC_TIME_SEC, for DEFAULT_ITER_NUM iterations
    rates = DEFAULT_WORKLOAD_PATTERN * DEFAULT_ITER_NUM
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

    if not Path("query_trace.csv").exists():
        LOG.error("Missing query_trace.csv at CWD after running OLTP TPCC")
        return False

    return True


class DataLoader:
    INTERVAL_MS = 500000
    DATA_TS_IDX = 1
    MS_PER_SEC = 1000000
    # First 35 seconds are for loading
    LOAD_PHASE_NUM_DATA = int(MS_PER_SEC / INTERVAL_MS * 35)

    # Number of data points in a sequence
    SEQ_LEN = 10 * MS_PER_SEC // INTERVAL_MS

    # Number of data points for the horizon
    HORIZON_LEN = 30 * MS_PER_SEC // INTERVAL_MS

    # Number of data points for testing set
    TEST_DATA_SIZE = 2 * SEQ_LEN + HORIZON_LEN

    def __init__(self, query_trace_file: str = "query_trace.csv") -> None:
        self.query_trace_file = query_trace_file

        self.seq_len = self.SEQ_LEN
        self.horizon_len = self.HORIZON_LEN
        self.scaler = MinMaxScaler(feature_range=(-1, 1))
        try:
            self._load_data()
        except Exception as e:
            LOG.error(f"Failed to load data: {e}")

    def _load_data(self):
        # Load data from the files
        with open(query_trace_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = np.array(
                [[int(r['query_id']), int(r[' timestamp'])] for r in reader])

            data = self._aggregate_data(data, self.INTERVAL_MS)

            self.train_raw_data, self.test_raw_data = self._split_data(data)

            self.train_norm_data = self._normalize_data(self.train_raw_data)

    def _normalize_data(self, data: np.ndarray) -> np.ndarray:
        norm_data = self.scaler.fit_transform(data.reshape(-1, 1))
        return norm_data

    def _aggregate_data(self, data: np.ndarray, interval_ms: int) -> Optional[np.ndarray]:
        start_t = data[0][TS_IDX]
        end_t = data[-1][TS_IDX]

        if end_t - start_t <= 1:
            return None

        # Number of data points
        num_buckets = (end_t - start_t - 1) // interval_ms + 1

        new_data = np.zeros(num_buckets)

        for i in range(len(data)):
            t = data[i][TS_IDX]
            bi = (t - start_t) // interval

            new_data[bi] += 1

        new_data = new_data[:self.LOAD_PHASE_NUM_DATA]
        return new_data

    def _split_data(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        self.test_set_size = self.TEST_DATA_SIZE

        # First part as the training set
        train_raw_data = data[:-self.test_set_size]

        # Last part as the testing set
        test_raw_data = data[-self.test_set_size:]

        return train_raw_data, test_raw_data

    def create_seqs(self) -> List[Tuple[torch.Tensor, torch.Tensor]]:
        seq_len = self.SEQ_LEN
        horizon = self.HORIZON_LEN
        input_data = torch.FloatTensor(self.train_norm_data).view(-1)
        seqs = []
        for i in range(len(input_data) - seq_len - horizon):
            seq = input_data[i:i + seq_len]
            label_i = i + seq_len + horizon
            label = input_data[label_i: label_i + 1]

            seqs.append((seq, label))

        return seqs

    def get_test_input(self) -> List[float]:
        test_input_norm = self._normalize_data(self.test_raw_data)
        test_input_list = test_input_norm.tolist()
        return test_input_list

    def stat_util(self):
        trace_start_ms = self.data[0][1]
        trace_end_ms = self.data[-1][1]
        trace_nsample = len(self.data)

        print(f"{trace_nsample} samples from {trace_start_ms} to {trace_end_ms}\n"
              f"{(trace_end_ms - trace_start_ms) / 1000000}seconds"
              )


class LSTM(nn.Module):
    """
    A simple LSTM that serves as a placeholder for the training model
    """

    def __init__(self, input_size=1, hidden_layer_size=100, output_size=1, save_path: str = None):
        super().__init__()
        self.hidden_layer_size = hidden_layer_size

        self.lstm = nn.LSTM(input_size, hidden_layer_size)

        self.linear = nn.Linear(hidden_layer_size, output_size)

        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                            torch.zeros(1, 1, self.hidden_layer_size))

        self.save_path = save_path

    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(
            input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(len(input_seq), -1))
        return predictions[-1]

    def train(self, data_loader: DataLoader, lr: float = 0.0001, epochs: int = 10) -> LSTM:
        train_seqs = data_loader.create_seqs()

        # Training specifics
        loss_function = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=lr)

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

        if self.save_path:
            with open(self.save_path, mode="wb") as f:
                pickle.dump(self.parameters(), f)

    def test(self, data_loader: DataLoader) -> List[float]:
        self.eval()
        test_input_list = data_loader.get_test_input()
        test_outputs = []
        for i in range(data_loader.SEQ_LEN):
            seq = torch.FloatTensor(test_input_list[i:i + SEQ_LEN])
            with torch.no_grad():
                model.hidden = (torch.zeros(1, 1, model.hidden_layer_size),
                                torch.zeros(1, 1, model.hidden_layer_size))
                test_outputs.append(model(seq).item())

        return test_outputs


if __name__ == "__main__":
    argp = argparse.ArgumentParser(description="Query Load Forecaster")

    # Generation stage related options
    argp.add_argument("--gen_data", default=True)
    argp.add_argument("--trace_file", default="query_trace.csv")

    # Training stage related options
    argp.add_argument("--train", default=True)
    argp.add_argument("--model_save_path", default="model.pickle")

    # Testing stage related options
    argp.add_argument("--test", default=True)
    argp.add_argument("--model_load_path", default="model.pickle")

    args = argp.parse_args()

    if args.gen_data:
        gen_oltp_trace()

    if args.train:
        data_loder = DataLoader(args.trace_file)
        model = LSTM()
        model.train(data_loder)
