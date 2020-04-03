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


class OpUnit(enum.IntEnum):
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
    OP_INTEGER_PLUS_OR_MINUS = 6,
    OP_INTEGER_MULTIPLY = 7,
    OP_INTEGER_DIVIDE = 8,
    OP_INTEGER_COMPARE = 9,
    OP_DECIMAL_PLUS_OR_MINUS = 10,
    OP_DECIMAL_MULTIPLY = 11,
    OP_DECIMAL_DIVIDE = 12,
    OP_DECIMAL_COMPARE = 13,
    SEQ_SCAN = 14,
    IDX_SCAN = 15,
    HASHJOIN_BUILD = 16,
    HASHJOIN_PROBE = 17,
    AGG_BUILD = 18,
    AGG_ITERATE = 19,
    SORT_BUILD = 20,
    SORT_ITERATE = 21,
    INSERT = 22,
    UPDATE = 23,


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
