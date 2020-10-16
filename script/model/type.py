"""All the types (defined using Enum).

This should be the only module that you directly import classes, instead of the higher-level module.
"""
import enum

class Target(enum.IntEnum):
    """The output targets for the operating units
    """
    START_TIME = 0
    CPU_ID = 1,
    CPU_CYCLES = 2,
    INSTRUCTIONS = 3,
    CACHE_REF = 4,
    CACHE_MISS = 5,
    REF_CPU_CYCLES = 6,
    BLOCK_READ = 7,
    BLOCK_WRITE = 8,
    MEMORY_B = 9,
    ELAPSED_US = 10


class OpUnit(enum.IntEnum):
    """The enum for all the operating units

    For each operating unit, the first upper case name should be used in the codebase,
    and the second lower case name (alias) is to match the string identifier from the csv data file
    """
    GC = 0,
    LOG_SERIALIZER_TASK = 1,
    DISK_LOG_CONSUMER_TASK = 2,
    TXN_BEGIN = 3,
    TXN_COMMIT = 4,

    # Execution engine opunits
    OUTPUT = 6,
    OP_INTEGER_PLUS_OR_MINUS = 7,
    OP_INTEGER_MULTIPLY = 8,
    OP_INTEGER_DIVIDE = 9,
    OP_INTEGER_COMPARE = 10,
    OP_DECIMAL_PLUS_OR_MINUS = 11,
    OP_DECIMAL_MULTIPLY = 12,
    OP_DECIMAL_DIVIDE = 13,
    OP_DECIMAL_COMPARE = 14,
    SEQ_SCAN = 15,
    IDX_SCAN = 16,
    HASHJOIN_BUILD = 17,
    HASHJOIN_PROBE = 18,
    AGG_BUILD = 19,
    AGG_ITERATE = 20,
    SORT_BUILD = 21,
    SORT_ITERATE = 22,
    INSERT = 23,
    UPDATE = 24,
    DELETE = 25,
    CREATE_INDEX = 26,
    CREATE_INDEX_MAIN = 27,
    PARALLEL_MERGE_HASHJOIN = 28,
    PARALLEL_MERGE_AGGBUILD = 29,
    PARALLEL_SORT_STEP = 30,
    PARALLEL_SORT_MERGE_STEP = 31


class ExecutionFeature(enum.IntEnum):
    # Debugging information
    QUERY_ID = 0,
    PIPELINE_ID = 1,

    # # features
    NUM_FEATURES = 2,
    FEATURES = 3,

    # input features
    EXEC_MODE = 4,
    NUM_ROWS = 5,
    KEY_SIZES = 6,
    NUM_KEYS = 7,
    EST_CARDINALITIES = 8,
    MEM_FACTOR = 9,
    NUM_LOOPS = 10,
    NUM_CONCURRENT = 11,

    # interval input features
    TXNS_DEALLOCATED = 12,
    TXNS_UNLINKED = 13,
    BUFFER_UNLINKED = 14,
    READONLY_UNLINKED = 15,
    INTERVAL = 16,


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
