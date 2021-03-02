#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import os
import logging
import tqdm
import math

from . import data_util
from ..info import data_info
from ..util import io_util
from ..type import OpUnit, Target, ExecutionFeature


def write_extended_data(output_path, symbol, index_value_list, data_map):
    # clear the content of the file
    open(output_path, 'w').close()

    io_util.write_csv_result(output_path, symbol, index_value_list)
    for key, value in data_map.items():
        io_util.write_csv_result(output_path, key, value)


def get_ou_runner_data(filename, model_results_path, txn_sample_rate, model_map={}, predict_cache={}, trim=0.2):
    """Get the training data from the ou runner

    :param filename: the input data file
    :param model_results_path: results log directory
    :param txn_sample_rate: sampling rate for the transaction OUs
    :param model_map: the map from OpUnit to the ou model
    :param predict_cache: cache for the ou model prediction
    :param trim: % of too high/too low anomalies to prune
    :return: the list of Data for execution operating units
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return _txn_get_ou_runner_data(filename, model_results_path, txn_sample_rate)
    if "execution" in filename:
        # Handle the execution data
        return _execution_get_ou_runner_data(filename, model_map, predict_cache, trim)
    if "gc" in filename or "log" in filename:
        # Handle of the gc or log data with interval-based conversion
        return _interval_get_ou_runner_data(filename, model_results_path)

    return _default_get_ou_runner_data(filename)


def _default_get_ou_runner_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename, skipinitialspace=True)
    headers = list(df.columns.values)
    data_info.instance.parse_csv_header(headers, False)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values

    return [OpUnitData(OpUnit[file_name.upper()], x, y)]


def _txn_get_ou_runner_data(filename, model_results_path, txn_sample_rate):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    # prepending a column of ones as the base transaction data feature
    base_x = pd.DataFrame(data=np.ones((df.shape[0], 1), dtype=int))
    df = pd.concat([base_x, df], axis=1)
    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.instance.TARGET_CSV_INDEX[data_info.instance.Target.START_TIME]].values
    cpu_ids = df.iloc[:, data_info.instance.TARGET_CSV_INDEX[data_info.instance.Target.CPU_ID]].values

    logging.info("Loaded file: {}".format(OpUnit[file_name.upper()]))

    # change the data based on the interval for the periodically invoked operating units
    prediction_path = "{}/{}_txn_converted_data.csv".format(model_results_path, file_name)
    io_util.create_csv_file(prediction_path, [""])

    interval = data_info.instance.CONTENDING_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    interval_id_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
            interval_id_map[rounded_time] = set()
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])
        interval_id_map[rounded_time].add(cpu_ids[i])

    # Construct the new data
    x_list = []
    y_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Concatenate the number of different threads
        x_new = np.concatenate((x_new, [len(interval_id_map[rounded_time])]))
        if txn_sample_rate > 0:
            x_new *= 100 / txn_sample_rate
        x_list.append(x_new)
        # The prediction is the average behavior
        y_list.append(np.average(interval_y_map[rounded_time], axis=0))
        io_util.write_csv_result(prediction_path, rounded_time, np.concatenate((x_list[-1], y_list[-1])))

    return [OpUnitData(OpUnit[file_name.upper()], np.array(x_list), np.array(y_list))]


def _interval_get_ou_runner_data(filename, model_results_path):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename, skipinitialspace=True)
    headers = list(df.columns.values)
    data_info.instance.parse_csv_header(headers, False)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.instance.RAW_TARGET_CSV_INDEX[Target.START_TIME]].values
    logging.info("Loaded file: {}".format(OpUnit[file_name.upper()]))

    # change the data based on the interval for the periodically invoked operating units
    prediction_path = "{}/{}_interval_converted_data.csv".format(model_results_path, file_name)
    io_util.create_csv_file(prediction_path, [""])

    interval = data_info.instance.PERIODIC_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])

    # Construct the new data
    x_list = []
    y_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Keep the interval parameter the same
        # TODO: currently the interval parameter is always the last. Change the hard-coding later
        x_new[-1] /= len(interval_x_map[rounded_time])
        x_list.append(x_new)
        # The prediction is the average behavior
        y_list.append(np.average(interval_y_map[rounded_time], axis=0))
        io_util.write_csv_result(prediction_path, rounded_time, np.concatenate((x_list[-1], y_list[-1])))

    return [OpUnitData(OpUnit[file_name.upper()], np.array(x_list), np.array(y_list))]


def _execution_get_ou_runner_data(filename, model_map, predict_cache, trim):
    """Get the training data from the ou runner

    :param filename: the input data file
    :param model_map: the map from OpUnit to the ou model
    :param predict_cache: cache for the ou model prediction
    :param trim: % of too high/too low anomalies to prune
    :return: the list of Data for execution operating units
    """

    # Get the ou runner data for the execution engine
    data_map = {}
    raw_data_map = {}
    input_output_boundary = math.nan
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        indexes = next(reader)
        data_info.instance.parse_csv_header(indexes, True)
        features_vector_index = data_info.instance.raw_features_csv_index[ExecutionFeature.FEATURES]
        raw_boundary = data_info.instance.raw_features_csv_index[data_info.instance.INPUT_OUTPUT_BOUNDARY]
        input_output_boundary = len(data_info.instance.input_csv_index)

        for line in reader:
            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i, d in enumerate(line) if i >= raw_boundary]
            data = list(map(data_util.convert_string_to_numeric, record))
            x_multiple = data[:input_output_boundary]
            y_merged = np.array(data[-data_info.instance.MINI_MODEL_TARGET_NUM:])

            # Get the opunits located within
            opunits = []
            features = line[features_vector_index].split(';')
            for idx, feature in enumerate(features):
                opunit = OpUnit[feature]
                x_loc = [v[idx] if type(v) == list else v for v in x_multiple]
                if opunit in model_map:
                    key = [opunit] + x_loc
                    if tuple(key) not in predict_cache:
                        predict = model_map[opunit].predict(np.array(x_loc).reshape(1, -1))[0]
                        predict_cache[tuple(key)] = predict
                        assert len(predict) == len(y_merged)
                        y_merged = y_merged - predict
                    else:
                        predict = predict_cache[tuple(key)]
                        assert len(predict) == len(y_merged)
                        y_merged = y_merged - predict

                    y_merged = np.clip(y_merged, 0, None)
                else:
                    opunits.append((opunit, x_loc))

            if len(opunits) > 1:
                raise Exception('Unmodelled OperatingUnits detected: {}'.format(opunits))

            # Record into predict_cache
            key = tuple([opunits[0][0]] + opunits[0][1])
            if key not in raw_data_map:
                raw_data_map[key] = []
            raw_data_map[key].append(y_merged)

    # Postprocess the raw_data_map -> data_map
    # We need to do this here since we need to have seen all the data
    # before we can start pruning. This step is done here so dropped
    # data don't actually become a part of the model.
    for key in raw_data_map:
        len_vec = len(raw_data_map[key])
        raw_data_map[key].sort(key=lambda x: x[-1])

        # compute how much to trim
        trim_side = trim * len_vec
        low = int(math.ceil(trim_side))
        high = len_vec - low
        if low >= high:
            # if bounds are bad, just take the median
            raw_data_map[key] = np.median(raw_data_map[key], axis=0)
        else:
            # otherwise, x% trimmed mean
            raw_data_map[key] = np.average(raw_data_map[key][low:high], axis=0)

        # Expose the singular data point
        opunit = key[0]
        if opunit not in data_map:
            data_map[opunit] = []

        predict = raw_data_map[key]
        predict_cache[key] = predict
        data_map[opunit].append(list(key[1:]) + list(predict))

    data_list = []
    for opunit, values in data_map.items():
        np_value = np.array(values)
        x = np_value[:, :input_output_boundary]
        y = np_value[:, -data_info.instance.MINI_MODEL_TARGET_NUM:]
        data_list.append(OpUnitData(opunit, x, y))

    return data_list


class OpUnitData:
    """
    The class that stores data and provides basic functions to manipulate the training data for the operating unit
    """

    def __init__(self, opunit, x, y):
        """

        :param opunit: The opunit that the data is related to
        :param x: The input feature
        :param y: The outputs
        """
        self.opunit = opunit
        self.x = x
        self.y = y
