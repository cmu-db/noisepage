import numpy as np

from ..info import hardware_info


class InterferenceImpactData:
    """
    The class used to store the information for the interference impact model training and prediction
    """

    def __init__(self, data, resource_data_list):
        """
        :param data: The target GroupedOpUnitData to measure and predict
        :param resource_data_list: The InterferenceResourceData list in the interval that the target GroupedOpUnitData
                overlap with
        """
        self.target_grouped_op_unit_data = data
        self.resource_data_list = resource_data_list

        # Derive the same_core_x feature
        cpu_id = data.cpu_id
        physical_core_num = hardware_info.PHYSICAL_CORE_NUM
        same_core_x_list = [d.x_list[cpu_id - physical_core_num if cpu_id > physical_core_num else cpu_id] for d
                            in resource_data_list]
        same_core_x = np.average(same_core_x_list, axis=0)
        self.resource_util_same_core_x = same_core_x

        # Derive the x feature
        x_list = [d.x for d in resource_data_list]
        x = np.average(x_list, axis=0)
        self.x = x

    def get_y_pred(self):
        y_pred_list = [d.y_pred for d in self.resource_data_list]
        y_pred = np.average(y_pred_list, axis=0)
        return y_pred


class InterferenceResourceData:
    """
    The class used to store the information for the interference resource model training and prediction
    """

    def __init__(self, start_time, x_list, x, y):
        """
        :param start_time: for the interval to measure the resource
        :param x_list: the list of normalized estimated resource usage per core
        :param x: input feature to predict y
        :param y: the normalized estimated total resource numbers
        """
        self.start_time = start_time
        self.x_list = x_list
        self.x = x
        self.y = y
        self.y_pred = None
