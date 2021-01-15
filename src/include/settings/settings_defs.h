#include "settings/settings_common.h"  // NOLINT

// clang-format off
// SETTING_<type>(name, description, default_value, min_value, max_value, is_mutable, callback_fn)

// Terrier port
SETTING_int(
    port,
    "Terrier port (default: 15721)",
    15721,
    1024,
    65535,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Preallocated connection handler threads and maximum number of connected clients
SETTING_int(
    connection_thread_count,
    "Preallocated connection handler threads and maximum number of connected clients (default: 4)",
    4,
    1,
    256,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Path to socket file for Unix domain sockets
SETTING_string(
    uds_file_directory,
    "The directory for the Unix domain socket (default: /tmp/)",
    "/tmp/",
    false,
    noisepage::settings::Callbacks::NoOp
)

// RecordBufferSegmentPool size limit
SETTING_int(
    record_buffer_segment_size,
    "The maximum number of record buffer segments in the system. (default: 100000)",
    100000,
    1,
    1000000000,
    true,
    noisepage::settings::Callbacks::BufferSegmentPoolSizeLimit
)

// RecordBufferSegmentPool reuse limit
SETTING_int(
    record_buffer_segment_reuse,
    "The minimum number of record buffer segments to keep allocated in the system (default: 10000)",
    10000,
    1,
    1000000000,
    true,
    noisepage::settings::Callbacks::BufferSegmentPoolReuseLimit
)

// BlockStore for catalog size limit
SETTING_int(
    block_store_size,
    "The maximum number of storage blocks for the catalog. (default: 100000)",
    100000,
    1,
    1000000000,
    true,
    noisepage::settings::Callbacks::BlockStoreSizeLimit
)

// BlockStore for catalog reuse limit
SETTING_int(
    block_store_reuse,
    "The minimum number of storage blocks for the catalog to keep allocated (default: 1000)",
    1000,
    1,
    1000000000,
    true,
    noisepage::settings::Callbacks::BlockStoreReuseLimit
)

// Garbage collector thread interval
SETTING_int(
    gc_interval,
    "Garbage collector thread interval (us) (default: 1000)",
    1000,
    1,
    10000,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Write ahead logging
SETTING_bool(
    wal_enable,
    "Whether WAL is enabled (default: true)",
    true,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Path to log file for WAL
SETTING_string(
    wal_file_path,
    "The path to the log file for the WAL (default: wal.log)",
    "wal.log",
    false,
    noisepage::settings::Callbacks::NoOp
)

// Number of buffers log manager can use to buffer logs
SETTING_int64(
    wal_num_buffers,
    "The number of buffers the log manager uses to buffer logs to hand off to log consumer(s) (default: 100)",
    100,
    2,
    10000,
    true,
    noisepage::settings::Callbacks::WalNumBuffers
)

// Log Serialization interval
SETTING_int(
    wal_serialization_interval,
    "Log serialization task interval (us) (default: 100)",
    100,
    1,
    10000,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Log file persisting interval
SETTING_int(
    wal_persist_interval,
    "Log file persisting interval (us) (default: 100)",
    100,
    1,
    10000,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Optimizer timeout
SETTING_int(task_execution_timeout,
            "Maximum allowed length of time (in ms) for task execution step of optimizer, "
            "assuming one plan has been found (default 5000)",
            5000, 1000, 60000, false, noisepage::settings::Callbacks::NoOp)

// Parallel Execution
SETTING_bool(
    parallel_execution,
    "Whether parallel execution for scans is enabled",
    true,
    true,
    noisepage::settings::Callbacks::NoOp
)

// Log file persisting threshold
SETTING_int64(
    wal_persist_threshold,
    "Log file persisting write threshold (bytes) (default: 1MB)",
    (1 << 20) /* 1MB */,
    (1 << 12) /* 4KB */,
    (1 << 24) /* 16MB */,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    extra_float_digits,
    "Sets the number of digits displayed for floating-point values. (default : 1)",
    1,
    -15,
    3,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    workload_forecast_interval,
    "Interval to be used to break query traces into WorkloadForecastSegment. (default : 10000000, unit: ns)",
    10000000,
    10000000,
    1000000000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    pilot_interval,
    "Interval of Pilot Planning Invocation when planning enabled. (default : 1000000, unit: ns)",
    1000000,
    1000000,
    10000000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    metrics,
    "Metrics sub-system for various components (default: true).",
    true,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    use_metrics_thread,
    "Use a thread for the metrics sub-system (default: true).",
    true,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    use_pilot_thread,
    "Use a thread for the pilot (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    pilot_planning,
    "Start planning in pilot (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::PilotEnablePlanning
)

SETTING_bool(
    logging_metrics_enable,
    "Metrics collection for the Logging component (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsLogging
)

SETTING_bool(
    transaction_metrics_enable,
    "Metrics collection for the TransactionManager component (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsTransaction
)

SETTING_bool(
    gc_metrics_enable,
    "Metrics collection for the GarbageCollector component (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsGC
)

SETTING_bool(
    query_trace_metrics_enable,
    "Metrics collection for Query Traces (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsQueryTrace
)

SETTING_bool(
    execution_metrics_enable,
    "Metrics collection for the Execution component (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsExecution
)

SETTING_bool(
    pipeline_metrics_enable,
    "Metrics collection for the ExecutionEngine pipelines (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::MetricsPipeline
)

SETTING_int(
    pipeline_metrics_interval,
    "Sampling rate of metrics collection for the ExecutionEngine pipelines with 0 = 100%, 1 = 50%, "
    "9 = 10%, X = 1/(X+1)% (default: 9 for 10%).",
    9,
    0,
    10,
    true,
    noisepage::settings::Callbacks::MetricsPipelineSamplingInterval
)

SETTING_bool(
    bind_command_metrics_enable,
    "Metrics collection for the bind command.",
    false,
    true,
    noisepage::settings::Callbacks::MetricsBindCommand
)

SETTING_bool(
    execute_command_metrics_enable,
    "Metrics collection for the execute command.",
    false,
    true,
    noisepage::settings::Callbacks::MetricsExecuteCommand
)

SETTING_bool(
    use_query_cache,
    "Extended Query protocol caches physical plans and generated code after first execution. Warning: bugs with DDL changes.",
    true,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    compiled_query_execution,
    "Compile queries to native machine code using LLVM, rather than relying on TPL interpretation (default: false).",
    false,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    application_name,
    "The name of the application (default: NO_NAME)",
    "NO_NAME",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    transaction_isolation,
    "The default isolation level (default: TRANSACTION_READ_COMMITTED)",
    "TRANSACTION_READ_COMMITTED",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    num_parallel_execution_threads,
    "Number of threads for parallel query execution (default: 1)",
    1,
    1,
    128,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    counters_enable,
    "Whether to use counters (default: false)",
    false,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    messenger_enable,
    "Whether to enable the messenger (default: false)",
    false,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    model_server_enable,
    "Whether to enable the ModelServerManager (default: false)",
    false,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Relative path assuming binary locate at PROJECT_ROOT/build/bin/, and model_server.py at PROJECT_ROOT/script/model
SETTING_string(
    model_server_path,
    "The python model server script to invoke (default: ../../script/model/model_server.py)",
    "../../script/model/model_server.py",
    false,
    noisepage::settings::Callbacks::NoOp
)

// Save path of the model relative to the build path (model saved at ${BUILD_ABS_PATH} + SAVE_PATH)
SETTING_string(
    model_save_path,
    "Save path of the model relative to the build path (default: ../script/model/terrier_model_server_trained/mini_model_test.pickle)",
    "/../script/model/terrier_model_server_trained/mini_model_test.pickle",
    false,
    noisepage::settings::Callbacks::NoOp
)
    // clang-format on
