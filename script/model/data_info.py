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

arithmetic_feature_index = {
    ArithmeticFeature.EXEC_NUMBER: 0,
    ArithmeticFeature.EXEC_MODE: 1,
}

# The position of the name of the operating unit in the execution engine csv metrics file
execution_csv_name_pos = 0

# total number of outputs
target_num = 9
metrics_output_num = 11

arithmetic_opunits = {OpUnit.INT_ADD, OpUnit.INT_MULTIPLY, OpUnit.INT_DIVIDE, OpUnit.INT_GREATER,
                      OpUnit.REAL_ADD, OpUnit.REAL_MULTIPLY, OpUnit.REAL_DIVIDE, OpUnit.REAL_GREATER}
