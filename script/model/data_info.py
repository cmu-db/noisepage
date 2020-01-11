from type import Target, OpUnit, ArithmeticFeature

# The index for different targets in the output
target_csv_index = {
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
arithmetic_feature_index = {
    ArithmeticFeature.EXEC_NUMBER: 0,
    ArithmeticFeature.EXEC_MODE: 1,
}

# The position of the name of the operating unit in the execution engine csv metrics file
execution_csv_name_pos = 0

# total number of outputs
metrics_output_num = 11

# prediction targets in the mini models
mini_model_target_list = [Target.CPU_CYCLE, Target.INSTRUCTION, Target.CACHE_REF, Target.CACHE_MISS, Target.CPU_TIME,
                          Target.BLOCK_READ, Target.BLOCK_WRITE, Target.MEMORY_B, Target.ELAPSED_US]
# the number of prediction targets in the mini models
mini_model_target_num = len(mini_model_target_list)

# All the opunits of arithmetic operations
arithmetic_opunits = {OpUnit.INT_ADD, OpUnit.INT_MULTIPLY, OpUnit.INT_DIVIDE, OpUnit.INT_GREATER,
                      OpUnit.REAL_ADD, OpUnit.REAL_MULTIPLY, OpUnit.REAL_DIVIDE, OpUnit.REAL_GREATER}

# The index for the tuple num feature in the operating units that has that feature
tuple_num_index = 0
