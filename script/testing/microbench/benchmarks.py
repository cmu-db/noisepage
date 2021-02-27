from .constants import DEFAULT_FAILURE_THRESHOLD

# The benchmark names and their associated threasholds
BENCHMARKS_TO_RUN = {
    "catalog_benchmark": 20,
    "data_table_benchmark": 75,
    "garbage_collector_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "large_transaction_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "index_wrapper_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "logging_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "recovery_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "large_transaction_metrics_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "logging_metrics_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "tuple_access_strategy_benchmark": 15,
    "tpcc_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "bwtree_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "bplustree_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "cuckoomap_benchmark": DEFAULT_FAILURE_THRESHOLD,
    "parser_benchmark": 20,
    "slot_iterator_benchmark": DEFAULT_FAILURE_THRESHOLD,
}
