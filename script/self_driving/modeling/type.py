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
    OP_REAL_PLUS_OR_MINUS = 11,
    OP_REAL_MULTIPLY = 12,
    OP_REAL_DIVIDE = 13,
    OP_REAL_COMPARE = 14,
    OP_VARCHAR_COMPARE = 15,

    SEQ_SCAN = 20,
    IDX_SCAN = 21,
    HASHJOIN_BUILD = 22,
    HASHJOIN_PROBE = 23,
    AGG_BUILD = 24,
    AGG_ITERATE = 25,
    SORT_BUILD = 26,
    SORT_TOPK_BUILD = 27,
    SORT_ITERATE = 28,
    INSERT = 29,
    UPDATE = 30,
    DELETE = 31,
    CREATE_INDEX = 32,
    CREATE_INDEX_MAIN = 33,
    PARALLEL_MERGE_HASHJOIN = 34,
    PARALLEL_MERGE_AGGBUILD = 35,
    PARALLEL_SORT_STEP = 36,
    PARALLEL_SORT_MERGE_STEP = 37
    INDEX_INSERT = 38
    INDEX_DELETE = 39

    # Networking opunits
    BIND_COMMAND = 40,
    EXECUTE_COMMAND = 41


class ExecutionFeature(enum.IntEnum):
    # Debugging information
    QUERY_ID = 0,
    PIPELINE_ID = 1,

    # # features
    NUM_FEATURES = 2,
    FEATURES = 3,

    # input features
    CPU_FREQ = 4,
    EXEC_MODE = 5,
    NUM_ROWS = 6,
    KEY_SIZES = 7,
    NUM_KEYS = 8,
    EST_CARDINALITIES = 9,
    MEM_FACTOR = 10,
    NUM_LOOPS = 11,
    NUM_CONCURRENT = 12,

    # interval input features
    TXNS_DEALLOCATED = 13,
    TXNS_UNLINKED = 14,
    BUFFER_UNLINKED = 15,
    READONLY_UNLINKED = 16,
    INTERVAL = 17,


class ConcurrentCountingMode(enum.Enum):
    """How to identify the concurrent running operations (for a GroupedOpUnitData)
    """
    EXACT = 0,
    ESTIMATED = 1,
    INTERVAL = 2,
