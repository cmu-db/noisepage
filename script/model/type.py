"""All the types (defined using Enum).

This should be the only module that you directly import classes, instead of the higher-level module.
"""
import enum


class Target(enum.Enum):
    """The output targets for the operating units
    """
    START_TIME = 0
    CPU_ID = 1,
    CPU_CYCLE = 2,
    INSTRUCTION = 3,
    CACHE_REF = 4,
    CACHE_MISS = 5,
    CPU_TIME = 6,
    BLOCK_READ = 7,
    BLOCK_WRITE = 8,
    MEMORY_B = 9,
    ELAPSED_US = 10,


class OpUnit(enum.Enum):
    """The enum for all the operating units

    For each operating unit, the first upper case name should be used in the codebase,
    and the second lower case name (alias) is to match the string identifier from the csv data file
    """
    GC_DEALLOC = 0,
    gc_deallocate = 0,
    GC_UNLINK = 1,
    gc_unlink = 1,
    LOG_SERIAL = 2,
    log_serializer_task = 2,
    LOG_CONSUME = 3,
    disk_log_consumer_task = 3,
    TXN_BEGIN = 4,
    txn_begin = 4,
    TXN_COMMIT = 5,
    txn_commit = 5,

    # Execution engine opunits
    SEQ_SCAN = 6,
    HASHJOIN_BUILD = 7,
    HASHJOIN_PROBE = 8,
    AGG_BUILD = 9,
    AGG_ITERATE = 10,
    SORT_BUILD = 11,
    SORT_ITERATE = 12,
    OP_INTEGER_PLUS_OR_MINUS = 13,
    OP_INTEGER_MULTIPLY = 14,
    OP_INTEGER_DIVIDE = 15,
    OP_INTEGER_COMPARE = 16,
    OP_DECIMAL_PLUS_OR_MINUS = 17,
    OP_DECIMAL_MULTIPLY = 18,
    OP_DECIMAL_DIVIDE = 19,
    OP_DECIMAL_COMPARE = 20,


class ArithmeticFeature(enum.Enum):
    """The input fields of the arithmetic operating units
    """
    EXEC_NUMBER = 0,
    EXEC_MODE = 1,


class ConcurrentCountingMode(enum.Enum):
    """How to identify the concurrent running operations (for a GroupedOpUnitData)
    """
    EXACT = 0,
    ESTIMATED = 1,
    INTERVAL = 2,
