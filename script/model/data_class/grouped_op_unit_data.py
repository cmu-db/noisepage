import csv
import numpy as np
import copy
import tqdm
import pandas as pd
import os
import logging

from data_class import data_util
from info import data_info
import global_model_config

from type import ConcurrentCountingMode, OpUnit, Target, ExecutionFeature


def get_grouped_op_unit_data(filename, warmup_period, ee_sample_interval, txn_sample_interval,
                             network_sample_interval):
    """Get the training data from the global model

    :param filename: the input data file
    :param warmup_period: warmup period for pipeline data
    :param ee_sample_interval: sampling interval for the EE OUs
    :param txn_sample_interval: sampling interval for the transaction OUs
    :param network_sample_interval: sampling interval for the network OUs
    :return: the list of global model data
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return _txn_get_mini_runner_data(filename, txn_sample_interval)
    if "pipeline" in filename:
        # Special handle of the pipeline execution data
        return _pipeline_get_grouped_op_unit_data(filename, warmup_period, ee_sample_interval)
    if "gc" in filename or "log" in filename:
        # Handle of the gc or log data with interval-based conversion
        return _interval_get_grouped_op_unit_data(filename)
    if "command" in filename:
        # Handle networking OUs
        return _default_get_global_data(filename, network_sample_interval)

    return _default_get_global_data(filename)


def _default_get_global_data(filename, sample_interval=0):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values

    # Construct the new data
    opunit = OpUnit[file_name.upper()]
    data_list = []

    for i in range(x.shape[0]):
        data_list.append(GroupedOpUnitData("{}".format(file_name), [(opunit, x[i])], y[i], sample_interval))
    return data_list


def _txn_get_mini_runner_data(filename, txn_sample_interval):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    # prepending a column of ones as the base transaction data feature
    base_x = pd.DataFrame(data=np.ones((df.shape[0], 1), dtype=int))
    df = pd.concat([base_x, df], axis=1)
    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.instance.target_csv_index[data_info.instance.Target.START_TIME]].values
    cpu_ids = df.iloc[:, data_info.instance.target_csv_index[data_info.instance.Target.CPU_ID]].values

    logging.info("Loaded file: {}".format(OpUnit[file_name.upper()]))

    interval = data_info.instance.CONTENDING_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    interval_cpu_id_map = {}
    interval_start_time_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
            interval_cpu_id_map[rounded_time] = []
            interval_start_time_map[rounded_time] = []
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])
        interval_cpu_id_map[rounded_time].append(cpu_ids[i])
        interval_start_time_map[rounded_time].append(start_times[i])

    # Construct the new data
    opunit = OpUnit[file_name.upper()]
    data_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Concatenate the number of different threads
        x_new = np.concatenate((x_new, [len(set(interval_cpu_id_map[rounded_time]))]))
        x_new *= txn_sample_interval + 1
        # Change all the opunits in the group for this interval to be the new feature
        opunits = [(opunit, x_new)]
        # The prediction is the average behavior
        y_new = np.average(interval_y_map[rounded_time], axis=0)
        n = len(interval_x_map[rounded_time])
        for i in range(n):
            metrics = np.concatenate(([interval_start_time_map[rounded_time][i]],
                                      [interval_cpu_id_map[rounded_time][i]],
                                      y_new))
            data_list.append(GroupedOpUnitData("{}".format(file_name), opunits, metrics, txn_sample_interval))

    return data_list


def _pipeline_get_grouped_op_unit_data(filename, warmup_period, ee_sample_interval):
    # Get the global running data for the execution engine
    start_time = None

    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        features_vector_index = data_info.instance.raw_features_csv_index[ExecutionFeature.FEATURES]
        input_output_boundary = data_info.instance.raw_features_csv_index[data_info.instance.INPUT_OUTPUT_BOUNDARY]
        input_end_boundary = len(data_info.instance.input_csv_index)

        for line in reader:
            # extract the time
            cpu_time = line[data_info.instance.raw_target_csv_index[Target.START_TIME]]
            if start_time is None:
                start_time = cpu_time

            if int(cpu_time) - int(start_time) < warmup_period * 1000000:
                continue

            sample_interval = ee_sample_interval

            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i,d in enumerate(line) if i >= input_output_boundary]
            data = list(map(data_util.convert_string_to_numeric, record))
            x_multiple = data[:input_end_boundary]
            metrics = np.array(data[-data_info.instance.METRICS_OUTPUT_NUM:])

            # Get the opunits located within
            opunits = []
            features = line[features_vector_index].split(';')
            concurrency = 0
            for idx, feature in enumerate(features):
                opunit = OpUnit[feature]
                x_loc = [v[idx] if type(v) == list else v for v in x_multiple]
                if x_loc[data_info.instance.input_csv_index[ExecutionFeature.NUM_ROWS]] == 0:
                    logging.info("Skipping {} OU with 0 tuple num".format(opunit.name))
                    continue

                if opunit == OpUnit.CREATE_INDEX:
                    concurrency = x_loc[data_info.instance.CONCURRENCY_INDEX]
                    # TODO(lin): we won't do sampling for CREATE_INDEX. We probably should encapsulate this when
                    #  generating the data
                    sample_interval = 0

                # TODO(lin): skip the main thing for interference model for now
                if opunit == OpUnit.CREATE_INDEX_MAIN:
                    continue

                opunits.append((opunit, x_loc))

            if len(opunits) == 0:
                continue

            # TODO(lin): Again, we won't do sampling for TPCH queries (with the assumption that the query id < 10).
            #  Should encapsulate this wit the metrics
            if int(line[0]) < 10:
                sample_interval = 0

            data_list.append(GroupedOpUnitData("q{} p{}".format(line[0], line[1]), opunits, np.array(metrics),
                                               sample_interval, concurrency))

    return data_list


def _interval_get_grouped_op_unit_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename, skipinitialspace=True)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.instance.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.instance.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.instance.target_csv_index[Target.START_TIME]].values
    cpu_ids = df.iloc[:, data_info.instance.target_csv_index[Target.CPU_ID]].values
    interval = data_info.instance.PERIODIC_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    interval_cpu_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])
        interval_cpu_map[rounded_time] = cpu_ids[i]

    # Construct the new data
    opunit = OpUnit[file_name.upper()]
    data_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Keep the interval parameter the same
        # TODO: currently the interval parameter is always the last. Change the hard-coding later
        x_new[-1] /= len(interval_x_map[rounded_time])
        # Change all the opunits in the group for this interval to be the new feature
        opunits = [(opunit, x_new)]
        # The prediction is the average behavior
        y_new = np.average(interval_y_map[rounded_time], axis=0)
        n = len(interval_x_map[rounded_time])
        for i in range(n):
            metrics = np.concatenate(([rounded_time + i * interval // n], [interval_cpu_map[rounded_time]], y_new))
            data_list.append(GroupedOpUnitData("{}".format(file_name), opunits, metrics))

    return data_list


class GroupedOpUnitData:
    """
    The class that stores the information about a group of operating units measured together
    """
    def __init__(self, name, opunit_features, metrics, sample_interval=0, concurrency=0):
        """
        :param name: The name of the data point (e.g., could be the pipeline identifier)
        :param opunit_features: The list of opunits and their inputs for this event
        :param metrics: The runtime metrics
        :param sample_interval: The sampling interval for this OU group
        :param concurrency: The number of concurrency for this contending (parallel) OU group
        """
        self.name = name
        self.opunit_features = opunit_features
        self.y = metrics[-data_info.instance.MINI_MODEL_TARGET_NUM:]
        self.y_pred = None
        index_map = data_info.instance.target_csv_index
        self.start_time = metrics[index_map[Target.START_TIME]]
        self.end_time = self.start_time + self.y[index_map[Target.ELAPSED_US]] - 1
        self.cpu_id = int(metrics[index_map[Target.CPU_ID]])
        self.sample_interval = sample_interval
        self.concurrency = concurrency

    def get_start_time(self, concurrent_counting_mode):
        """Get the start time for this group for counting the concurrent operations

        :param concurrent_counting_mode: ConcurrentCountingMode type
        :return: the start time
        """
        start_time = None
        if concurrent_counting_mode is ConcurrentCountingMode.EXACT:
            start_time = self.start_time
        if concurrent_counting_mode is ConcurrentCountingMode.ESTIMATED:
            start_time = self.start_time
        if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
            start_time = self.start_time + global_model_config.INTERVAL_START
        return start_time

    def get_end_time(self, concurrent_counting_mode):
        """Get the end time for this group for counting the concurrent operations

        :param concurrent_counting_mode: ConcurrentCountingMode type
        :return: the end time
        """
        end_time = None
        if concurrent_counting_mode is ConcurrentCountingMode.EXACT:
            end_time = self.end_time
        if concurrent_counting_mode is ConcurrentCountingMode.ESTIMATED:
            end_time = self.start_time + self.y_pred[data_info.instance.target_csv_index[Target.ELAPSED_US]] - 1
        if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
            end_time = self.start_time + global_model_config.INTERVAL_START + global_model_config.INTERVAL_SIZE
        return end_time
