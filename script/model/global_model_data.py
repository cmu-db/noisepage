import grouped_op_unit_data


class GlobalModelData:
    """
    The class used to store the information for the global model training and prediction
    """
    def __init__(self):
        """
        """
        self.target_grouped_op_unit_data = None
        self.concurrent_group_op_unit_data_list = None
        self.concurrent_group_op_unit_ratio_list = None
        self.global_resource_utilization_y = None
        self.global_resource_utilization_y_pred = None
