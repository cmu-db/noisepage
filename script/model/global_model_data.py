class GlobalImpactData:
    """
    The class used to store the information for the global impact model training and prediction
    """
    def __init__(self, data, resource_data, same_core_x):
        """
        :param data: The target GroupedOpUnitData to measure and predict
        :param resource_data: The GlobalResourceData in the interval that the target GroupedOpUnitData belongs to
        :param same_core_x: the estimated resource utilization feature on the same core as the target GroupedOpUnitData
        """
        self.target_grouped_op_unit_data = data
        self.resource_data = resource_data
        self.resource_util_same_core_x = same_core_x


class GlobalResourceData:
    """
    The class used to store the information for the global resource model training and prediction
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
