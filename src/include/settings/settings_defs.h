#include "settings/settings_common.h"  // NOLINT

// clang-format off
// SETTING_<type>(name, description, default_value, min_value, max_value, is_mutable, callback_fn)

// NoisePage port
SETTING_int(
    port,
    "NoisePage port (default: 15721)",
    15721,
    1024,
    65535,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    network_identity,
    "The identity of this NoisePage instance (default: primary)",
    "primary",
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
    "The maximum number of record buffer segments in the system. (default: 1000000)",
    1000000,
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
    "The maximum number of storage blocks for the catalog. (default: 1000000)",
    1000000,
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

// Asynchronous commit txns when WAL is enabled
SETTING_bool(
    wal_async_commit_enable,
    "Enable commit confirmation before results are durable in the WAL. (default: false)",
    false,
    false,
    noisepage::settings::Callbacks::NoOp
)

// Asynchronous replication instead of synchronous replication, if replication is enabled.
SETTING_bool(
    async_replication_enable,
    "Asynchronous replication instead of synchronous replication, if replication is enabled. (default: false)",
    false,
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
    true,
    noisepage::settings::Callbacks::WalSerializationInterval
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
    "Interval to be used to break query traces into WorkloadForecastSegment. (default : 1000000, unit: micro-second)",
    1000000,
    1000000,
    1000000000000,
    true,

    // When this callback is implemented in the near-fuure, do not
    // forget to update QueryTraceMetricRawData::QUERY_SEGMENT_INTERVAL
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    sequence_length,
    "Length of a planning data sequence. (default: 10, unit: workload_forecast_intervals)",
    10,
    1,
    1000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    horizon_length,
    "Length of the planning horizon. (default: 30, unit: workload_forecast_intervals)",
    30,
    1,
    1000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    pilot_interval,
    "Interval of Pilot Planning Invocation when planning enabled. (default : 1000000, unit: micro-second)",
    1000000,
    1000000,
    10000000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    forecast_train_interval,
    "Interval of Pilot Forecast Train Invocation when planning enabled. (default : 120000000, unit: micro-second)",
    120000000,
    120000000,
    10000000000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int64(
    pilot_memory_constraint,
    "Maximum amount of memory allowed for the pilot to plan. (default : 1000000000, unit: byte)",
    1000000000,
    0,
    100000000000,
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
    enable_seq_tuning,
    "Use sequence tuning instead of monte carlo tree search for pilot planning (default: false).",
    false,
    true,
    noisepage::settings::Callbacks::NoOp
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

SETTING_string(
    query_trace_metrics_output,
    "Output type for Query Traces Metrics (default: CSV, values: NONE, CSV, DB, CSV_AND_DB)",
    "CSV",
    true,
    noisepage::settings::Callbacks::MetricsQueryTraceOutput
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
    pipeline_metrics_sample_rate,
    "Sampling rate of metrics collection for the ExecutionEngine pipelines.",
    10,
    0,
    100,
    true,
    noisepage::settings::Callbacks::MetricsPipelineSampleRate
)

SETTING_int(
  logging_metrics_sample_rate,
  "Sampling rate of metrics collection for logging.",
  100,
  0,
  100,
  true,
  noisepage::settings::Callbacks::MetricsLoggingSampleRate
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
    true,
    noisepage::settings::Callbacks::CompiledQueryExecution
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

SETTING_int(
    messenger_port,
    "NoisePage messenger port (default: 9022)",
    9022,
    1024,
    65535,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    replication_enable,
    "Whether to enable replication (default: false)",
    false,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    replication_port,
    "NoisePage replication port (default: 15445)",
    15445,
    1024,
    65535,
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    replication_hosts_path,
    "The path to the hosts.conf file for replication (default: ./replication.config)",
    "./replication.config",
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
    ou_model_save_path,
    "Save path of the OU model relative to the build path (default: ou_model_map.pickle)",
    "ou_model_map.pickle",
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    interference_model_save_path,
    "Save path of the forecast model relative to the build path (default: interference_direct_model.pickle)",
    "interference_direct_model.pickle",
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    forecast_model_save_path,
    "Save path of the forecast model relative to the build path (default: forecast_model.pickle)",
    "forecast_model.pickle",
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    forecast_sample_limit,
    "Limit on number of samples for workload forecasting",
    5,
    0,
    100,
    true,
    noisepage::settings::Callbacks::ForecastSampleLimit
)

SETTING_int(
    task_pool_size,
    "Number of threads available to the task manager",
    1,
    1,
    32,
    true,
    noisepage::settings::Callbacks::TaskPoolSize
)

SETTING_string(
    startup_ddl_path,
    "Path to startup DDL (default: bin/startup.sql)",
    "bin/startup.sql",
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    bytecode_handlers_path,
    "The path to the bytecode handlers bitcode file (default: ./bytecode_handlers_ir.bc)",
    "./bytecode_handlers_ir.bc",
    false,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    train_forecast_model,
    "Train the forecast model (the value is not relevant and has no effect during startup).",
    false,
    true,
    noisepage::settings::Callbacks::TrainForecastModel
)

SETTING_bool(
    train_interference_model,
    "Train the interference model (the value is not relevant and has no effect during startup).",
    false,
    true,
    noisepage::settings::Callbacks::TrainInterferenceModel
)

SETTING_string(
    interference_model_input_path,
    "Input path to the directory containing training the interference model",
    "concurrent_runner_input/",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    interference_model_train_methods,
    "Methods to be used for training the interference model (comma delimited)",
    "rf",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    interference_model_train_timeout,
    "Timeout in milliseconds for training the interference model (default: 2 minutes)",
    120000,
    1000,
    600000,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    interference_model_pipeline_sample_rate,
    "Sampling rate of pipeline metrics OUs (0 is ignored)",
    2,
    0,
    10,
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_bool(
    train_ou_model,
    "Train the OU model (the value is not relevant and has no effect during startup).",
    false,
    true,
    noisepage::settings::Callbacks::TrainOUModel
)

SETTING_string(
    ou_model_input_path,
    "Input path to the directory containing training the OU model",
    "ou_runner_input/",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_string(
    ou_model_train_methods,
    "Methods to be used for training the OU model (comma delimited)",
    "lr,rf,gbm,nn",
    true,
    noisepage::settings::Callbacks::NoOp
)

SETTING_int(
    ou_model_train_timeout,
    "Timeout in milliseconds for training the OU model (default: 2 minutes)",
    120000,
    1000,
    600000,
    true,
    noisepage::settings::Callbacks::NoOp
)


// The default log level is unspecified because people may have inserted calls
// to set_level directly in the codebase, which would make a default inaccurate.
#define SETTINGS_LOG_LEVEL(component)                      \
SETTING_string(                                            \
    log_level_##component,                                 \
    "Set the log level for the component.",                \
    "(unspecified)",                                       \
    true,                                                  \
    noisepage::settings::Callbacks::LogLevelSet##component \
)

SETTINGS_LOG_LEVEL(binder)
SETTINGS_LOG_LEVEL(catalog)
SETTINGS_LOG_LEVEL(common)
SETTINGS_LOG_LEVEL(execution)
SETTINGS_LOG_LEVEL(index)
SETTINGS_LOG_LEVEL(messenger)
SETTINGS_LOG_LEVEL(metrics)
SETTINGS_LOG_LEVEL(modelserver)
SETTINGS_LOG_LEVEL(network)
SETTINGS_LOG_LEVEL(optimizer)
SETTINGS_LOG_LEVEL(parser)
SETTINGS_LOG_LEVEL(replication)
SETTINGS_LOG_LEVEL(selfdriving)
SETTINGS_LOG_LEVEL(settings)
SETTINGS_LOG_LEVEL(storage)
SETTINGS_LOG_LEVEL(transaction)

#undef SETTINGS_LOG_LEVEL
    // clang-format on
