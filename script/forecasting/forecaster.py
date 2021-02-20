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
EVAL_DATA_SIZE = SEQ_LEN + 2 * HORIZON_LEN

argp = argparse.ArgumentParser(description="Query Load Forecaster")

# Generation stage related options
argp.add_argument(
    "--gen_data",
    default=False,
    action="store_true",
    help="If specified, OLTP benchmark would be downloaded and built to generate the query trace data")
argp.add_argument(
    "--gen_data_only",
    default=False,
    action="store_true",
    help="If specified, only data generation will ensue")
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


if __name__ == "__main__":
    args = argp.parse_args()

    if args.test_file is None:
        # Generate OLTP trace file
        if args.gen_data:
            gen_oltp_trace(
                tpcc_weight=args.tpcc_weight,
                tpcc_rates=args.tpcc_rates,
                pattern_iter=args.pattern_iter)

            if args.gen_data_only:
                exit()

            trace_file = DEFAULT_QUERY_TRACE_FILE
        else:
            trace_file = args.trace_file

        # Parse models arguments
        sys.path.insert(0, str((Path.cwd() / '..').absolute()))
        from self_driving.forecasting.forecaster import Forecaster, parse_model_config
        models_kwargs = parse_model_config(args.models, args.models_config)

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
        sys.path.insert(0, str((Path.cwd() / '..').absolute()))
        from self_driving.forecasting.forecaster import Forecaster, parse_model_config

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
            LOG.info(f"[Query: {qid}] pred={ts}")
