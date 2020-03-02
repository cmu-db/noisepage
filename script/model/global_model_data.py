import grouped_op_unit_data


class GlobalModelData:
    """
    The class used to store the information for the global model training and prediction
    """
    def __init__(self, data, concurrent_data_list, x, same_core_x, y):
        """
        :param data:
        :param concurrent_data_list:
        :param x:
        :param same_core_x:
        :param y:
        """
        self.target_grouped_op_unit_data = data
        self.concurrent_group_op_unit_data_list = concurrent_data_list
        self.global_resource_util_x = x
        self.global_resource_util_same_core_x = same_core_x
        self.global_resource_util_y = y
        self.global_resource_util_y_pred = None
