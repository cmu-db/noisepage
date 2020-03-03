class GlobalModelData:
    """
    The class used to store the information for the global model training and prediction
    """
    def __init__(self, data, concurrent_data_list, x, same_core_x, y):
        """
        :param data: The target GroupedOpUnitData to measure and predict
        :param concurrent_data_list: The other GroupedOpUnitDatas that are running concurrently
        :param x: the estimated resource utilization feature (for predicting the global resource utilization)
        :param same_core_x: the estimated resource utilization feature on the same core as the target GroupedOpUnitData
        :param y: the global resource utilization
        """
        self.target_grouped_op_unit_data = data
        self.concurrent_group_op_unit_data_list = concurrent_data_list
        self.global_resource_util_x = x
        self.global_resource_util_same_core_x = same_core_x
        self.global_resource_util_y = y
        self.global_resource_util_y_pred = None
