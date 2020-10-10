// SETTING_<type>(name, description, default_value, min_value, max_value, is_mutable, callback_fn)

// Terrier port
SETTING_int(
    port,
    "Terrier port (default: 15721)",
    15721,
    1024,
    65535,
    false,
    terrier::settings::Callbacks::NoOp
)

// Preallocated connection handler threads and maximum number of connected clients
SETTING_int(
    connection_thread_count,
    "Preallocated connection handler threads and maximum number of connected clients (default: 4)",
    4,
    1,
    256,
    false,
    terrier::settings::Callbacks::NoOp
)

// Path to socket file for Unix domain sockets
SETTING_string(
    uds_file_directory,
    "The directory for the Unix domain socket (default: /tmp/)",
    "/tmp/",
    false,
    terrier::settings::Callbacks::NoOp
)

// RecordBufferSegmentPool size limit
SETTING_int(
    record_buffer_segment_size,
    "The maximum number of record buffer segments in the system. (default: 100000)",
    100000,
    1,
    100000000,
    true,
    terrier::settings::Callbacks::BufferSegmentPoolSizeLimit
)

// RecordBufferSegmentPool reuse limit
SETTING_int(
    record_buffer_segment_reuse,
    "The minimum number of record buffer segments to keep allocated in the system (default: 10000)",
    10000,
    1,
    1000000,
    true,
    terrier::settings::Callbacks::BufferSegmentPoolReuseLimit
)

// BlockStore for catalog size limit
SETTING_int(
    block_store_size,
    "The maximum number of storage blocks for the catalog. (default: 100000)",
    100000,
    1,
    1000000,
    true,
    terrier::settings::Callbacks::BlockStoreSizeLimit
)

// BlockStore for catalog reuse limit
SETTING_int(
    block_store_reuse,
    "The minimum number of storage blocks for the catalog to keep allocated (default: 1000)",
    1000,
    1,
    1000000,
    true,
    terrier::settings::Callbacks::BlockStoreReuseLimit
)

// Garbage collector thread interval
SETTING_int(
    gc_interval,
    "Garbage collector thread interval (us) (default: 1000)",
    1000,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Write ahead logging
SETTING_bool(
    wal_enable,
    "Whether WAL is enabled (default: true)",
    true,
    false,
    terrier::settings::Callbacks::NoOp
)

// Path to log file for WAL
SETTING_string(
    wal_file_path,
    "The path to the log file for the WAL (default: wal.log)",
    "wal.log",
    false,
    terrier::settings::Callbacks::NoOp
)

// Number of buffers log manager can use to buffer logs
SETTING_int64(
    wal_num_buffers,
    "The number of buffers the log manager uses to buffer logs to hand off to log consumer(s) (default: 100)",
    100,
    2,
    10000,
    true,
    terrier::settings::Callbacks::WalNumBuffers
)

// Log Serialization interval
SETTING_int(
    wal_serialization_interval,
    "Log serialization task interval (us) (default: 100)",
    100,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Log file persisting interval
SETTING_int(
    wal_persist_interval,
    "Log file persisting interval (us) (default: 100)",
    100,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Optimizer timeout
SETTING_int(task_execution_timeout,
            "Maximum allowed length of time (in ms) for task execution step of optimizer, "
            "assuming one plan has been found (default 5000)",
            5000, 1000, 60000, false, terrier::settings::Callbacks::NoOp)

// Parallel Execution
SETTING_bool(
    parallel_execution,
    "Whether parallel execution for scans is enabled",
    true,
    true,
    terrier::settings::Callbacks::NoOp
)

// Log file persisting threshold
SETTING_int64(
    wal_persist_threshold,
    "Log file persisting write threshold (bytes) (default: 1MB)",
    (1 << 20) /* 1MB */,
    (1 << 12) /* 4KB */,
    (1 << 24) /* 16MB */,
    false,
    terrier::settings::Callbacks::NoOp
)

SETTING_int(
    extra_float_digits,
    "Sets the number of digits displayed for floating-point values. (default : 1)",
    1,
    -15,
    3,
    true,
    terrier::settings::Callbacks::NoOp
)

SETTING_bool(
    metrics,
    "Metrics sub-system for various components (default: true).",
    true,
    false,
    terrier::settings::Callbacks::NoOp
)

SETTING_bool(
    metrics_logging,
    "Metrics collection for the Logging component (default: false).",
    false,
    true,
    terrier::settings::Callbacks::MetricsLogging
)

SETTING_bool(
    metrics_transaction,
    "Metrics collection for the TransactionManager component (default: false).",
    false,
    true,
    terrier::settings::Callbacks::MetricsTransaction
)

SETTING_bool(
    metrics_gc,
    "Metrics collection for the GarbageCollector component (default: false).",
    false,
    true,
    terrier::settings::Callbacks::MetricsGC
)

SETTING_bool(
    metrics_execution,
    "Metrics collection for the Execution component (default: false).",
    false,
    true,
    terrier::settings::Callbacks::MetricsExecution
)

SETTING_bool(
    metrics_pipeline,
    "Metrics collection for the ExecutionEngine pipelines (default: false).",
    false,
    true,
    terrier::settings::Callbacks::MetricsPipeline
)

SETTING_bool(
    metrics_bind_command,
    "Metrics collection for the bind command.",
    false,
    true,
    terrier::settings::Callbacks::MetricsBindCommand
)

SETTING_bool(
    metrics_execute_command,
    "Metrics collection for the execute command.",
    false,
    true,
    terrier::settings::Callbacks::MetricsExecuteCommand
)

SETTING_bool(
    use_query_cache,
    "Extended Query protocol caches physical plans and generated code after first execution. Warning: bugs with DDL changes.",
    true,
    false,
    terrier::settings::Callbacks::NoOp
)

SETTING_bool(
    compiled_query_execution,
    "Compile queries to native machine code using LLVM, rather than relying on TPL interpretation (default: false).",
    false,
    false,
    terrier::settings::Callbacks::NoOp
)

SETTING_string(
    application_name,
    "The name of the application (default: NO_NAME)",
    "NO_NAME",
    true,
    terrier::settings::Callbacks::NoOp
)

SETTING_string(
    transaction_isolation,
    "The default isolation level (default: TRANSACTION_READ_COMMITTED)",
    "TRANSACTION_READ_COMMITTED",
    true,
    terrier::settings::Callbacks::NoOp
)
