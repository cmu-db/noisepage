# Default pattern: High -> Low  ...
DEFAULT_TPCC_RATE = 10000
DEFAULT_WORKLOAD_PATTERN = [DEFAULT_TPCC_RATE, DEFAULT_TPCC_RATE // 10]

# Default time, runs 30 second for a work phase
DEFAULT_TPCC_TIME_SEC = 30

# Run the workload pattern for 2 iterations
DEFAULT_ITER_NUM = 2

# Default weights for TPC-C transactions (NewOrder, Payment, OrderStatus, Delivery, StockLevel).
DEFAULT_TPCC_WEIGHTS = "45,43,4,4,4"
# Default number of TPC-C terminals.
DEFAULT_TPCC_TERMINALS = 4
# Default number of TPC-C warehouses.
DEFAULT_TPCC_SCALE_FACTOR = 4
# Default query mode for TPC-C.
DEFAULT_TPCC_QUERY_MODE = "extended"

# Enable query trace collection, it will produce a query_trace.csv at CWD
DEFAULT_SERVER_ARGS = {
    "server_args": {}
}

# Default query_trace file name
DEFAULT_QUERY_TRACE_FILE = "query_trace.csv"

# Default query_trace file name
DEFAULT_PIPELINE_METRICS_FILE = "pipeline.csv"

# Default pipeline metrics sample rate (percentage)
DEFAULT_PIPELINE_METRICS_SAMPLE_RATE = 2

# Default DB user
DEFAULT_DB_USER = "noisepage"
