import csv
import numpy as np
import copy
import tqdm

from data_class import data_util
from info import data_info, query_info
import global_model_config

from type import Target, ConcurrentCountingMode, OpUnit


def get_grouped_op_unit_data(filename):
    """Get the training data from the global model

    :param filename: the input data file
    :return: the list of global model data
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return []
    if "execution" in filename:
        # Special handle of the execution data
        return _execution_get_grouped_op_unit_data(filename)
    if "pipeline" in filename:
        # Special handle of the pipeline execution data
        return _pipeline_get_grouped_op_unit_data(filename)

    return _default_get_global_data(filename)


def _execution_get_grouped_op_unit_data(filename):
    # Get the global running data for the execution engine
    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in tqdm.tqdm(reader):
            # The first element is always the query/pipeline identifier
            identifier = line[0]
            if identifier in query_info.FEATURE_MAP:
                # Need to deep copy since we're going to add the execution mode after it
                opunit_features = copy.deepcopy(query_info.FEATURE_MAP[identifier])
                # Execution mode is the second element for now...
                mode = int(line[1])
                for opunit_feature in opunit_features:
                    opunit_feature[1].append(mode)
                line_data = list(map(int, line[2:]))
                data_list.append(GroupedOpUnitData(line[0], opunit_features, np.array(line_data)))

    return data_list


def _pipeline_get_grouped_op_unit_data(filename):
    # Get the global running data for the execution engine
    execution_mode_index = data_info.RAW_EXECUTION_MODE_INDEX
    features_vector_index = data_info.RAW_FEATURES_VECTOR_INDEX

    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i,d in enumerate(line) if i > features_vector_index]
            record.insert(data_info.EXECUTION_MODE_INDEX, line[execution_mode_index])
            data = list(map(data_util.convert_string_to_numeric, record))
            x_multiple = data[:data_info.RECORD_FEATURES_END]
            metrics = np.array(data[-data_info.METRICS_OUTPUT_NUM:])

            # Get the opunits located within
            opunits = []
            features = line[features_vector_index].split(';')
            for idx, feature in enumerate(features):
                if feature == 'LIMIT' or feature == 'PROJECTION':
                    continue
                opunit = OpUnit[feature]
                x_loc = [v[idx] if type(v) == list else v for v in x_multiple]
                opunits.append((opunit, x_loc))

            data_list.append(GroupedOpUnitData("{}".format(opunits), opunits, np.array(metrics)))

    return data_list


def _default_get_global_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    return []


class GroupedOpUnitData:
    """
    The class that stores the information about a group of operating units measured together
    """
    def __init__(self, name, opunit_features, metrics):
        """
        :param name: The name of the data point (e.g., could be the pipeline identifier)
        :param opunit_features: The list of opunits and their inputs for this event
        :param metrics: The runtime metrics
        """
        self.name = name
        self.opunit_features = opunit_features
        self.y = metrics[-data_info.MINI_MODEL_TARGET_NUM:]
        self.y_pred = None
        index_map = data_info.TARGET_CSV_INDEX
        self.start_time = metrics[index_map[Target.START_TIME]]
        self.end_time = self.start_time + self.y[index_map[Target.ELAPSED_US]] - 1
        self.cpu_id = metrics[index_map[Target.CPU_ID]]

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
            end_time = self.start_time + self.y_pred[data_info.TARGET_CSV_INDEX[Target.ELAPSED_US]] - 1
        if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
            end_time = self.start_time + global_model_config.INTERVAL_START + global_model_config.INTERVAL_SIZE
        return end_time
