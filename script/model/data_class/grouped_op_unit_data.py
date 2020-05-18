import csv
import numpy as np
import copy
import tqdm

from info import data_info, query_info
import global_model_config

from type import Target, ConcurrentCountingMode


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

                # get memory scaling factor
                mem_scaling_factor = 1.0
                if line[0] in query_info.MEM_ADJUST_MAP:
                    mem_scaling_factor = query_info.MEM_ADJUST_MAP[line[0]]

                # Execution mode is the second element for now...
                mode = int(line[1])
                for opunit_feature in opunit_features:
                    if opunit_feature[0] in data_info.MEM_ADJUST_OPUNITS:
                        opunit_feature[1].append(mem_scaling_factor)
                    else:
                        opunit_feature[1].append(1.0)
                    opunit_feature[1].append(mode)

                line_data = list(map(int, line[2:]))
                data_list.append(GroupedOpUnitData(line[0], opunit_features, np.array(line_data)))

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
