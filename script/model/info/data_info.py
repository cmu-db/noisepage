from type import OpUnit, ArithmeticFeature, ExecutionFeature

# Boundary from input feature -> outputs
# The field >= boundary is the inputs and outputs
INPUT_OUTPUT_BOUNDARY = ExecutionFeature.EXEC_MODE

# End of the input features
INPUT_END_BOUNDARY = ExecutionFeature.START_TIME

# The index for fields in the raw CSV data
RAW_CSV_INDEX = {}

# The index for different aspects of input feature
INPUT_CSV_INDEX = {}

# The index for different targets in the output
TARGET_CSV_INDEX = {}

# The mini model features for arithmetic operations
ARITHMETIC_FEATURE_INDEX = {
    ArithmeticFeature.EXEC_NUMBER: 0,
    ArithmeticFeature.EXEC_MODE: 1,
}

# The position of the name of the operating unit in the execution engine csv metrics file
EXECUTION_CSV_NAME_POSITION = 0

# total number of outputs
METRICS_OUTPUT_NUM = ExecutionFeature.ELAPSED_US - ExecutionFeature.START_TIME + 1

# prediction targets in the mini models
MINI_MODEL_TARGET_LIST = [ExecutionFeature.CPU_CYCLES, ExecutionFeature.INSTRUCTIONS, ExecutionFeature.CACHE_REF,
                          ExecutionFeature.CACHE_MISS, ExecutionFeature.REF_CPU_CYCLES, ExecutionFeature.BLOCK_READ,
                          ExecutionFeature.BLOCK_WRITE, ExecutionFeature.MEMORY_B, ExecutionFeature.ELAPSED_US]
# the number of prediction targets in the mini models
MINI_MODEL_TARGET_NUM = len(MINI_MODEL_TARGET_LIST)

# All the opunits of arithmetic operations
ARITHMETIC_OPUNITS = {OpUnit.OP_INTEGER_PLUS_OR_MINUS, OpUnit.OP_INTEGER_MULTIPLY, OpUnit.OP_INTEGER_DIVIDE, OpUnit.OP_INTEGER_COMPARE,
                      OpUnit.OP_DECIMAL_PLUS_OR_MINUS, OpUnit.OP_DECIMAL_MULTIPLY, OpUnit.OP_DECIMAL_DIVIDE, OpUnit.OP_DECIMAL_COMPARE}

# Operating units that need memory adjustment
MEM_ADJUST_OPUNITS = {OpUnit.SORT_BUILD, OpUnit.HASHJOIN_BUILD, OpUnit.AGG_BUILD}

# Default is to evaluate a model by using the elapsed time prediction error.
# For opunits included here, models should be evaluated using the memory error.
MEM_EVALUATE_OPUNITS = {OpUnit.CREATE_INDEX_MAIN}

# Size of a pointer on the target architecture
POINTER_SIZE = 8

# Interval for opunits that wake up periodically (us)
PERIODIC_OPUNIT_INTERVAL = 1000000

def parse_csv_header(header, raw_boundary=False):
    """Parses a CSV header for ExecutionFeature indexes

    :param header: array of CSV column headers
    :param raw_boundary: whether there is a raw boundary or not
    """
    for i, index in enumerate(header):
        RAW_CSV_INDEX[ExecutionFeature[index.upper()]] = i

    input_output_boundary = None
    if raw_boundary:
        # Computes the index that divides the unnecessary fields and the input/output
        input_output_boundary = RAW_CSV_INDEX[INPUT_OUTPUT_BOUNDARY]

    # Computes the index within input/output of where input splits from output
    output_boundary = RAW_CSV_INDEX[INPUT_END_BOUNDARY]

    for i, index in enumerate(header):
        if i >= output_boundary:
            TARGET_CSV_INDEX[ExecutionFeature[index.upper()]] = i - len(header)

        if input_output_boundary is not None and i >= input_output_boundary:
            INPUT_CSV_INDEX[ExecutionFeature[index.upper()]] = i - input_output_boundary
