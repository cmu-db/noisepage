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

# Operating units that need memory adjustment
MEM_ADJUST_OPUNITS = {OpUnit.SORT_BUILD, OpUnit.HASHJOIN_BUILD, OpUnit.AGG_BUILD}

# Default is to evaluate a model by using the elapsed time prediction error.
# For opunits included here, models should be evaluated using the memory error.
MEM_EVALUATE_OPUNITS = {OpUnit.CREATE_INDEX_MAIN}

# Index for tuple_num in the model input feature vector
TUPLE_NUM_INDEX = 0

# Index for cardinality in the model input feature vector
CARDINALITY_INDEX = 3

# Index for execution mode in the model input feature vector
EXECUTION_MODE_INDEX = 7

# Size of a pointer on the target architecture
POINTER_SIZE = 8

# Index of features vector in the raw input from CSV
RAW_FEATURES_VECTOR_INDEX = 4

# Index of execution mode in the raw input from CSV
RAW_EXECUTION_MODE_INDEX = 2

# Index of cpu time in the raw input from CSV
RAW_CPU_TIME_INDEX = 12

# End index of model input feature vector
RECORD_FEATURES_END = 8

# Memory Scaling factor location in feature
RECORD_MEM_SCALE_OFFSET = -4

# Start index of metrics
RECORD_METRICS_START = MINI_MODEL_TARGET_NUM

# Interval for opunits that wake up periodically (us)
PERIODIC_OPUNIT_INTERVAL = 1000000
