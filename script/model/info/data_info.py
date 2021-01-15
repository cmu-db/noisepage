from type import OpUnit, Target, ExecutionFeature

# Boundary from input feature -> outputs
# The field >= boundary is the inputs and outputs
INPUT_OUTPUT_BOUNDARY = ExecutionFeature.CPU_FREQ

# End of the input features
INPUT_END_BOUNDARY = Target.START_TIME

# The index for miscellaneous + features in the raw CSV data
RAW_FEATURES_CSV_INDEX = {}

# The index for targets in the raw CSV data
RAW_TARGET_CSV_INDEX = {}

# The index for different aspects of input feature
INPUT_CSV_INDEX = {}

# The index for different targets in the output
TARGET_CSV_INDEX = {}

# The position of the name of the operating unit in the execution engine csv metrics file
EXECUTION_CSV_NAME_POSITION = 0

# total number of outputs
METRICS_OUTPUT_NUM = len(Target)

# prediction targets in the mini models
MINI_MODEL_TARGET_LIST = [Target.CPU_CYCLES, Target.INSTRUCTIONS, Target.CACHE_REF,
                          Target.CACHE_MISS, Target.REF_CPU_CYCLES, Target.BLOCK_READ,
                          Target.BLOCK_WRITE, Target.MEMORY_B, Target.ELAPSED_US]
# the number of prediction targets in the mini models
MINI_MODEL_TARGET_NUM = len(MINI_MODEL_TARGET_LIST)

# All the opunits of arithmetic operations
ARITHMETIC_OPUNITS = {OpUnit.OP_INTEGER_PLUS_OR_MINUS, OpUnit.OP_INTEGER_MULTIPLY, OpUnit.OP_INTEGER_DIVIDE,
                      OpUnit.OP_INTEGER_COMPARE,
                      OpUnit.OP_REAL_PLUS_OR_MINUS, OpUnit.OP_REAL_MULTIPLY, OpUnit.OP_REAL_DIVIDE,
                      OpUnit.OP_REAL_COMPARE}

# Operating units that need memory adjustment
MEM_ADJUST_OPUNITS = {OpUnit.SORT_BUILD, OpUnit.HASHJOIN_BUILD, OpUnit.AGG_BUILD}

# Default is to evaluate a model by using the elapsed time prediction error.
# For opunits included here, models should be evaluated using the memory error.
MEM_EVALUATE_OPUNITS = {OpUnit.CREATE_INDEX_MAIN}

# Size of a pointer on the target architecture
POINTER_SIZE = 8

# Interval for opunits that wake up periodically (us)
PERIODIC_OPUNIT_INTERVAL = 1000000

# Interval for contending opunits (us)
CONTENDING_OPUNIT_INTERVAL = 3000000

# OUs using cardinality estimation (may add noise to during prediction)
OUS_USING_CAR_EST = {OpUnit.AGG_BUILD, OpUnit.AGG_ITERATE, OpUnit.HASHJOIN_BUILD, OpUnit.HASHJOIN_PROBE,
                     OpUnit.SORT_BUILD, OpUnit.SORT_ITERATE}


def parse_csv_header(header, raw_boundary=False):
    """Parses a CSV header for ExecutionFeature indexes

    :param header: array of CSV column headers
    :param raw_boundary: whether there is a raw boundary or not
    """
    for i, index in enumerate(header):
        if index.upper() not in ExecutionFeature.__members__:
            RAW_TARGET_CSV_INDEX[Target[index.upper()]] = i
        else:
            RAW_FEATURES_CSV_INDEX[ExecutionFeature[index.upper()]] = i

    input_output_boundary = None
    if raw_boundary:
        # Computes the index that divides the unnecessary fields and the input/output
        input_output_boundary = RAW_FEATURES_CSV_INDEX[INPUT_OUTPUT_BOUNDARY]

    # Computes the index within input/output of where input splits from output
    output_boundary = RAW_TARGET_CSV_INDEX[INPUT_END_BOUNDARY]

    for i, index in enumerate(header):
        if i >= output_boundary:
            TARGET_CSV_INDEX[Target[index.upper()]] = i - len(header)
        elif input_output_boundary is not None and i >= input_output_boundary:
            INPUT_CSV_INDEX[ExecutionFeature[index.upper()]] = i - input_output_boundary
