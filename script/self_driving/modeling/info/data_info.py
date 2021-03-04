from ..type import OpUnit, Target, ExecutionFeature


class DataInfo:
    """
    The class that stores the position information of the data
    """

    def __init__(self):
        # Boundary from input feature -> outputs
        # The field >= boundary is the inputs and outputs
        self.INPUT_OUTPUT_BOUNDARY = ExecutionFeature.CPU_FREQ

        # End of the input features
        self.INPUT_END_BOUNDARY = Target.START_TIME

        # The index for miscellaneous + features in the raw CSV data
        self.raw_features_csv_index = {}

        # The index for targets in the raw CSV data
        self.raw_target_csv_index = {}

        # The index for different aspects of input feature
        self.input_csv_index = {}

        # The index for different targets in the output
        self.target_csv_index = {}

        # The position of the name of the operating unit in the execution engine csv metrics file
        self.EXECUTION_CSV_NAME_POSITION = 0

        # total number of outputs
        self.METRICS_OUTPUT_NUM = len(Target)

        # prediction targets in the mini models
        self.MINI_MODEL_TARGET_LIST = [Target.CPU_CYCLES, Target.INSTRUCTIONS, Target.CACHE_REF,
                                       Target.CACHE_MISS, Target.REF_CPU_CYCLES, Target.BLOCK_READ,
                                       Target.BLOCK_WRITE, Target.MEMORY_B, Target.ELAPSED_US]
        # the number of prediction targets in the mini models
        self.MINI_MODEL_TARGET_NUM = len(self.MINI_MODEL_TARGET_LIST)

        # All the opunits of arithmetic operations
        self.ARITHMETIC_OPUNITS = {OpUnit.OP_INTEGER_PLUS_OR_MINUS, OpUnit.OP_INTEGER_MULTIPLY,
                                   OpUnit.OP_INTEGER_DIVIDE,
                                   OpUnit.OP_INTEGER_COMPARE,
                                   OpUnit.OP_REAL_PLUS_OR_MINUS, OpUnit.OP_REAL_MULTIPLY, OpUnit.OP_REAL_DIVIDE,
                                   OpUnit.OP_REAL_COMPARE}

        # Operating units that need memory adjustment
        self.MEM_ADJUST_OPUNITS = {OpUnit.SORT_BUILD, OpUnit.HASHJOIN_BUILD, OpUnit.AGG_BUILD}

        # Default is to evaluate a model by using the elapsed time prediction error.
        # For opunits included here, models should be evaluated using the memory error.
        self.MEM_EVALUATE_OPUNITS = {OpUnit.CREATE_INDEX_MAIN}

        # Size of a pointer on the target architecture
        self.POINTER_SIZE = 8

        # Interval for opunits that wake up periodically (us)
        self.PERIODIC_OPUNIT_INTERVAL = 1000000

        # Interval for contending opunits (us)
        self.CONTENDING_OPUNIT_INTERVAL = 3000000

        # OUs using cardinality estimation (may add noise to during prediction)
        self.OUS_USING_CAR_EST = {OpUnit.AGG_BUILD, OpUnit.AGG_ITERATE, OpUnit.HASHJOIN_BUILD, OpUnit.HASHJOIN_PROBE,
                                  OpUnit.SORT_BUILD, OpUnit.SORT_ITERATE}

    def parse_csv_header(self, header, raw_boundary=False):
        """Parses a CSV header for ExecutionFeature indexes

        :param header: array of CSV column headers
        :param raw_boundary: whether there is a raw boundary or not
        """
        for i, index in enumerate(header):
            if index.upper() not in ExecutionFeature.__members__:
                self.raw_target_csv_index[Target[index.upper()]] = i
            else:
                self.raw_features_csv_index[ExecutionFeature[index.upper()]] = i

        input_output_boundary = None
        if raw_boundary:
            # Computes the index that divides the unnecessary fields and the input/output
            input_output_boundary = self.raw_features_csv_index[self.INPUT_OUTPUT_BOUNDARY]

        # Computes the index within input/output of where input splits from output
        output_boundary = self.raw_target_csv_index[self.INPUT_END_BOUNDARY]

        for i, index in enumerate(header):
            if i >= output_boundary:
                self.target_csv_index[Target[index.upper()]] = i - len(header)
            elif input_output_boundary is not None and i >= input_output_boundary:
                self.input_csv_index[ExecutionFeature[index.upper()]] = i - input_output_boundary


instance = DataInfo()
