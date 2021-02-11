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
import argparse
import json
import pickle
from functools import lru_cache
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

from ..testing.self_driving.constants import (DEFAULT_ITER_NUM,
                                              DEFAULT_QUERY_TRACE_FILE,
                                              DEFAULT_TPCC_WEIGHTS,
                                              DEFAULT_WORKLOAD_PATTERN)
from ..testing.self_driving.forecast import gen_oltp_trace
from ..testing.util.constants import LOG
from .cluster import QueryCluster
from .data_loader import DataLoader
from .models import ForecastModel, get_models

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
    TRAIN_DATA_IDX = 0
    TEST_DATA_IDX = 1

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

    def _make_seqs(self,
                   input_data: np.ndarray,
                   start: int,
                   end: int,
                   with_label: bool = False) -> List[Union[Tuple[np.ndarray,
                                                                 np.ndarray],
                                                           np.ndarray]]:
        """
        Create time-series sequences of fixed sequence length from a continuous range of time-series.
        :param input_data: Input time-series
        :param start: Start index (inclusive) of the first sequence to be made
        :param end:  End index (exclusive) of the last sequence to be made
        :param with_label: True if label in a certain horizon is added
        :return: Sequences of fixed length if with_label is False,
                or List of fixed length sequence and label if with_label is True
        """
        seq_len = self._seq_len
        horizon = self._horizon_len

        seq_start = start
        if with_label:
            # Reserve space for horizon
            seq_end = end - seq_len - horizon
        else:
            # Use all data for prediction
            seq_end = end - seq_len

        if seq_end <= seq_start:
            raise IndexError(f"Not enough data points to make sequences")

        seqs = []
        for i in range(seq_start, seq_end):
            seq = input_data[i:i + seq_len].reshape(-1, 1)

            # Look beyond the horizon to get the label
            if with_label:
                label_i = i + seq_len + horizon
                label = input_data[label_i: label_i + 1].reshape(1, -1)
                seqs.append((seq, label))
            else:
                seqs.append(seq)
        return seqs

    @lru_cache(maxsize=32)
    def _cluster_seqs(self,
                      cluster_id: int,
                      test_mode: bool = False,
                      with_label: bool = False) -> List[Union[Tuple[np.ndarray,
                                                                    np.ndarray],
                                                              np.ndarray]]:
        """
        Create time-series sequences of fixed sequence length from a continuous range of time-series. A cached wrapper
        over _make_seqs with different options.
        :param cluster_id: Cluster id
        :param test_mode: True if using test dataset, otherwise use the training dataset
        :param with_label: True if label (time-series data in a horizon from the sequence) is also added.
        :return: Sequences of fixed length if with_label is False,
                or List of fixed length sequence and label if with_label is True
        """
        if test_mode:
            input_data = self._cluster_data[cluster_id][self.TEST_DATA_IDX]
        else:
            input_data = self._cluster_data[cluster_id][self.TRAIN_DATA_IDX]

        seqs = self._make_seqs(
            input_data,
            0,
            len(input_data),
            with_label=with_label)
        return seqs

    def train(self, models_kwargs: Dict) -> List[List[ForecastModel]]:
        """
        :param models_kwargs: A dictionary of models' init arguments
        :return: List of models(a list of models) for each cluster.
        """
        models = []
        for cid in range(len(self._cluster_data)):
            cluster_models = get_models(models_kwargs)
            train_seqs = self._cluster_seqs(
                cid, test_mode=False, with_label=True)
            for model_name, model in cluster_models.items():
                # Fit the model
                model.fit(train_seqs)
                self.eval(cid, model)

            models.append(cluster_models)
        return models

    def eval(self, cid: int, model: ForecastModel) -> None:
        """
        Evaluate a fitted model on the test dataset.
        :param cid: Cluster id
        :param model: Model to use
        """
        eval_seqs = self._cluster_seqs(cid, test_mode=True, with_label=True)
        preds = []
        gts = []
        for seq, label in eval_seqs:
            pred = model.predict(seq)
            preds.append(pred)
            gts.append(label.item())

        # FIXME:
        # simple L2 norm for comparing the prediction and results
        l2norm = np.linalg.norm(np.array(preds) - np.array(gts))
        LOG.info(
            f"[{model.name}] has L2 norm(prediction, ground truth) = {l2norm}")

    def predict(self, cid: int, model: ForecastModel) -> Dict:
        """
        Output prediction on the test dataset, and segregate the predicted cluster time-series into individual queries
        :param cid: Cluser id
        :param model: Model to use
        :return: Dict of {query_id -> time-series}
        """
        test_seqs = self._cluster_seqs(cid, test_mode=True, with_label=False)
        preds = list([model.predict(seq) for seq in test_seqs])
        query_preds = self._clusters[cid].segregate(preds)

        return query_preds


def parse_model_config(model_names: Optional[List[str]],
                       models_config: Optional[str]) -> Dict:
    """
    Load models from
    :param model_names: List of model names
    :param models_config: JSON model config file
    :return: Merged model config Dict
    """

    model_kwargs = dict([(model_name, {}) for model_name in model_names])
    if models_config is not None:
        with open(models_config, 'r') as f:
            custom_config = json.load(f)
            # Simple and non-recursive merging of options
            model_kwargs.update(custom_config)

    if len(model_kwargs) < 1:
        raise ValueError("At least 1 model needs to be used.")

    return model_kwargs


if __name__ == "__main__":
    args = argp.parse_args()

    if args.test_file is None:
        # Parse models arguments
        models_kwargs = parse_model_config(args.models, args.models_config)

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

        models = forecaster.train(models_kwargs)

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
        query_pred = forecaster.predict(0, models[0][args.test_model])

        # TODO:
        # How are we consuming predictions?
        for qid, ts in query_pred.items():
            LOG.info(f"[Query: {qid}] pred={ts[:10]}")
