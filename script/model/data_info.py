from enum import Enum

# The index for different targets in the output
target_index = {
    "cpu_cycles": -11,
    "instructions": -10,
    "cache_ref": -9,
    "cache_miss": -8,
    "cpu_time": -7,
    "block_read": -6,
    "block_write": -5,
    "cpu_id": -4,
    "memory_b": -3,
    "elapsed_us": -2,
    "start_time": -1
}

# total number of outputs
target_num = 11

class OpUnit(Enum):
    '''
    The enum for all the operating units

    The second lower case name (alias) is to match the string identifier from the csv data file
    '''
    GC_DEALLOC = "gc_deallocate",
    GC_UNLINK = "gc_unlink",
    LOG_SERIAL = "log_serializer_task",
    LOG_CONSUME = 1,
    disk_log_consumer_task = 1,
    TXN_BEGIN = "txn_begin",
    TXN_COMMIT = "txn_commit",

    # Execution engine opunits
    SCAN = "scan",
    JOIN_BUILD = "joinbuild",
    JOIN_PROBE = "joinprobe",
    AGG_BUILD = "aggbuild",
    AGG_PROBE = "aggprobe",
    SORT_BUILD = "sortbuild",
    SORT_PROBE = "sortprobe",
    INT_ADD = "integeradd",
    INT_MULTIPLY = "integermultiply",
    INT_DIVIDE = "integerdivide",
    INT_GREATER = "integergreater",
    REAL_ADD = "realadd",
    REAL_MULTIPLY = "realmultiply",
    REAL_DIVIDE = "realdivide",
    REAL_GREATER = "realgreater",


execution_opunits = {OpUnit.INT_ADD, OpUnit.INT_MULTIPLY, OpUnit.INT_DIVIDE, OpUnit.INT_GREATER,
                     OpUnit.REAL_ADD, OpUnit.REAL_MULTIPLY, OpUnit.REAL_DIVIDE, OpUnit.REAL_GREATER}