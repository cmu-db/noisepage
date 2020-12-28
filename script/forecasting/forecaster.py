#!/usr/bin/env python3
"""
Main script for workload forecasting.
Example usage:
- Generate data (runs OLTP benchmark on the built database) and perform training, and save the trained model
    ./forecaster --gen_data --models=LSTM --model_save_path=model.pickle

- Use the trained models (LSTM) to generate predictions.
    ./forecaster --model_load_path=model.pickle --test_file=test_query.csv --test_model=LSTM


TODO:
    - Better metrics for training and prediction (currently not focusing on models' accuracy yet)
    - Multiple models (currently only simple-one-layer-untuned LSTM used)
    - API and interaction with Pilot
"""

from data_loader import DataLoader
from cluster import QueryCluster
from models import ForecastModel, get_models
from self_driving.constants import (
    DEFAULT_TPCC_WEIGHTS,
    DEFAULT_QUERY_TRACE_FILE,
    DEFAULT_WORKLOAD_PATTERN,
    DEFAULT_ITER_NUM)
from self_driving.forecast import gen_oltp_trace
from util.constants import LOG
from typing import List, Tuple, Dict, Optional
from functools import lru_cache
import numpy as np
import pickle
import json
import argparse
import sys
from pathlib import Path
sys.path.insert(0, str((Path.cwd() / '..' / 'testing').absolute()))


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
argp.add_argument("--models", nargs='+', type=str, help="Models to use")
argp.add_argument("--models_config", type=str, metavar="FILE",
                  help="Models and init arguments JSON config file")
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
argp.add_argument(
    "--test_model",
    type=str,
    help="Model to be used for forecasting"
)


class Forecaster:
    """
    A wrapper around various ForecastModels, that prepares training and evaluation data.
    """

    def __init__(
            self,
            trace_file: str,
            interval_us: int = INTERVAL_MICRO_SEC,
            test_mode: bool = False,
            eval_size: int = EVAL_DATA_SIZE,
            seq_len: int = SEQ_LEN,
            horizon_len: int = HORIZON_LEN) -> None:
        """
        Initializer
        :param trace_file: trace file for the forecaster
        :param interval_us: number of microseconds for the time-series interval
        :param test_mode: True If the Loader is for testing
        :param eval_size: Number of data points used for evaluation(testing)
        :param seq_len: Length of a sequence
        :param horizon_len: Horizon length
        """
        self._seq_len = seq_len
        self._horizon_len = horizon_len
        self._test_mode = test_mode
        self._eval_data_size = eval_size

        self._data_loader = DataLoader(
            query_trace_file=trace_file,
            interval_us=interval_us)

        self._make_clusters()

    def _make_clusters(self) -> None:
        """
        Extract data from the DataLoader and put them into different clusters.
        :return: None
        """
        # FIXME:
        # Assuming all the queries in the current trace file are from
        # the same cluster for now. A future TODO would have a clustering
        # process that separates traces into multiple clusters
        self._clusters = [QueryCluster(self._data_loader.get_ts_data())]
        self._cluster_data = []
        for cluster in self._clusters:
            # Aggregated time-series from the cluster
            data = cluster.get_timeseries()
            train_raw_data, test_raw_data = self._split_data(data)
            self._cluster_data.append((train_raw_data, test_raw_data))

    def _split_data(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Split the raw data into a training set, and a testing(evaluation) set.
        :param data: All the raw data
        :return: traing, test raw data set
        """
        if self._test_mode:
            self._test_set_size = len(data)
        else:
            self._test_set_size = self._eval_data_size
            if self._test_set_size > len(data):
                raise ValueError(
                    "Eval data size is too small. Not enough data points.")

        split_idx = len(data) - self._test_set_size
        # First part as the training set
        train_raw_data = data[:split_idx]

        # Last part as the testing set
        test_raw_data = data[split_idx:]

        return train_raw_data, test_raw_data

    @lru_cache(maxsize=32)
    def _cluster_train_seqs(
            self, cluster_id: int) -> List[Tuple[np.ndarray, np.ndarray]]:
        """
        Create training sequences.  Each training sample consists of:
         - A list of data points as input
         - A label (target) value in some horizon
        :return: training sequences
        """
        seq_len = self._seq_len
        horizon = self._horizon_len
        input_data, _test_data = self._cluster_data[cluster_id]

        seqs = []
        for i in range(len(input_data) - seq_len - horizon):
            seq = input_data[i:i + seq_len].reshape(-1, 1)

            # Look beyond the horizon to get the label
            label_i = i + seq_len + horizon
            label = input_data[label_i: label_i + 1].reshape(1, -1)

            seqs.append((seq, label))
        return seqs

    @lru_cache(maxsize=32)
    def _cluster_test_seqs(self, cluster_id: int) -> List[np.ndarray]:
        """
        Generate a list of test sequence for evaluation
        :return:  Test sequences for evaluation
        """
        _train_data, input_data = self._cluster_data[cluster_id]

        seqs = []
        for i in range(len(input_data) - self._seq_len):
            seq = input_data[i:i + self._seq_len].reshape(-1, 1)
            seqs.append(seq)

        return seqs

    def train(self, models: Dict) -> None:
        """
        :param models:  A Dict of models {name -> ForecastModels} to fit
        :return:
        """

        for cid in range(len(self._cluster_data)):
            train_seqs = self._cluster_train_seqs(cid)
            for _, model in models.items():
                model.fit(train_seqs)

    def eval(self, cid: int, model: ForecastModel) -> Dict:
        """
        Evaluate a model on the test dataset
        :param model: Model to evaluate
        :return: Each query's prediction stored in a Dict  {query_id: prediction}
        """
        test_data = self._cluster_test_seqs(cid)

        results = []
        for seq in test_data:
            pred = model.predict(seq)
            results.append(pred)

        # Conver the aggregated prediction to each query's prediction
        query_pred = self._clusters[cid].segregate(results)

        return query_pred


def init_models(model_names: Optional[List[str]],
                models_config: Optional[str]) -> List[ForecastModel]:
    """
    Load models from
    :param model_names: List of model names
    :param models_config: JSON model config file
    :return:
    """
    # Initialize training models
    if model_names is None or len(model_names) < 1:
        raise ValueError("At least 1 model needs to be used.")

    model_args = dict([(model_name, {}) for model_name in model_names])
    if models_config is not None:
        with open(models_config, 'r') as f:
            custom_config = json.load(f)
            # Simple and non-recursive merging of options
            model_args.update(custom_config)
    models = get_models(model_args)
    return models


if __name__ == "__main__":
    args = argp.parse_args()

    if args.test_file is None:
        # Load models
        models = init_models(args.models, args.models_config)

        # Generate OLTP trace file
        if args.gen_data:
            gen_oltp_trace(
                tpcc_weight=args.tpcc_weight,
                tpcc_rates=args.tpcc_rates,
                pattern_iter=args.pattern_iter)

            trace_file = DEFAULT_QUERY_TRACE_FILE
        else:
            trace_file = args.trace_file

        forecaster = Forecaster(
            trace_file=trace_file,
            interval_us=INTERVAL_MICRO_SEC,
            seq_len=args.seq_len,
            eval_size=args.eval_size,
            horizon_len=args.horizon_len)

        forecaster.train(models)

        # Save the model
        if args.model_save_path:
            with open(args.model_save_path, "wb") as f:
                pickle.dump(models, f)
    else:
        # Do inference on a trained model
        with open(args.model_load_path, "rb") as f:
            models = pickle.load(f)

        forecaster = Forecaster(
            trace_file=args.test_file,
            test_mode=True,
            interval_us=INTERVAL_MICRO_SEC,
            seq_len=args.seq_len,
            eval_size=args.eval_size,
            horizon_len=args.horizon_len)

        # FIXME:
        # Assuming all the queries in the current trace file are from
        # the same cluster for now
        query_pred = forecaster.eval(0, models[args.test_model])

        # TODO:
        # How are we evaluating the prediction, and where would it be consumed?
        for qid, ts in query_pred.items():
            LOG.info(f"[Query: {qid}] pred={ts[:10]}")
