from type import Target, OpUnit, ArithmeticFeature

# The index for different targets in the output
TARGET_CSV_INDEX = {
    Target.START_TIME: -11,
    Target.CPU_ID: -10,

    Target.CPU_CYCLE: -9,
    Target.INSTRUCTION: -8,
    Target.CACHE_REF: -7,
    Target.CACHE_MISS: -6,
    Target.CPU_TIME: -5,
    Target.BLOCK_READ: -4,
    Target.BLOCK_WRITE: -3,
    Target.MEMORY_B: -2,
    Target.ELAPSED_US: -1,
}

# The mini model features for arithmetic operations
ARITHMETIC_FEATURE_INDEX = {
    ArithmeticFeature.EXEC_NUMBER: 0,
    ArithmeticFeature.EXEC_MODE: 1,
}

# The position of the name of the operating unit in the execution engine csv metrics file
EXECUTION_CSV_NAME_POSITION = 0

# total number of outputs
METRICS_OUTPUT_NUM = 11

# prediction targets in the mini models
MINI_MODEL_TARGET_LIST = [Target.CPU_CYCLE, Target.INSTRUCTION, Target.CACHE_REF, Target.CACHE_MISS, Target.CPU_TIME,
                          Target.BLOCK_READ, Target.BLOCK_WRITE, Target.MEMORY_B, Target.ELAPSED_US]
# the number of prediction targets in the mini models
MINI_MODEL_TARGET_NUM = len(MINI_MODEL_TARGET_LIST)

# All the opunits of arithmetic operations
ARITHMETIC_OPUNITS = {OpUnit.OP_INTEGER_PLUS_OR_MINUS, OpUnit.OP_INTEGER_MULTIPLY, OpUnit.OP_INTEGER_DIVIDE, OpUnit.OP_INTEGER_COMPARE,
                      OpUnit.OP_DECIMAL_PLUS_OR_MINUS, OpUnit.OP_DECIMAL_MULTIPLY, OpUnit.OP_DECIMAL_DIVIDE, OpUnit.OP_DECIMAL_COMPARE}

MEM_ADJUST_OPUNITS = {OpUnit.SORT_BUILD, OpUnit.HASHJOIN_BUILD, OpUnit.AGG_BUILD}

# The index for the tuple num feature in the operating units that has that feature
TUPLE_NUM_INDEX = 0
CARDINALITY_INDEX = 3
EXECUTION_MODE_INDEX = 5
POINTER_SIZE = 8

# Raw line of execution CSV
RAW_FEATURES_VECTOR_INDEX = 4
RAW_EXECUTION_MODE_INDEX = 2
RECORD_FEATURES_END = 6
RECORD_METRICS_START = MINI_MODEL_TARGET_NUM
