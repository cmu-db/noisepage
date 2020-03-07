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
    SCAN = 6,
    scan = 6,
    JOIN_BUILD = 7,
    joinbuild = 7,
    JOIN_PROBE = 8,
    joinprobe = 8,
    AGG_BUILD = 9,
    aggbuild = 9,
    AGG_PROBE = 10,
    aggprobe = 10,
    SORT_BUILD = 11,
    sortbuild = 11,
    SORT_PROBE = 12,
    sortprobe = 12,
    INT_ADD = 13,
    integeradd = 13,
    INT_MULTIPLY = 14,
    integermultiply = 14,
    INT_DIVIDE = 15,
    integerdivide = 15,
    INT_GREATER = 16,
    integergreater = 16,
    REAL_ADD = 17,
    realadd = 17,
    REAL_MULTIPLY = 18,
    realmultiply = 18,
    REAL_DIVIDE = 19,
    realdivide = 19,
    REAL_GREATER = 20,
    realgreater = 20,


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
