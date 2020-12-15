## SanctionedSharedPtr usage in loggers.

All of the `spdlog`-related `subcomponent_logger` loggers are a `SanctionedSharedPtr` instance to various sinks,
ranging from an application-wide `spdlog::sinks::stdout_sink_mt` to custom `spdlog::logger` instances per class.

Ownership:
1. The shared pointers are owned by the `spdlog` third-party library.
2. The shared pointers are created in the `InitSubcomponentLogger()` functions.
3. The shared pointers are freed when `LoggersUtil::ShutDown()` is called.  

Memory management:
1. There are no memory leaks being hidden by making the shared pointers.
2. The shared pointers are necessary because `spdlog` has a global registry that stores and owns all the loggers.
   The registry hands shared pointers back out. This is a small lie, see note 1 below.
3. The shared pointers are only initialized in `subcomponent_logger.cpp::InitSubcomponentLogger()`.
   All functions merely access the shared pointers to invoke `spdlog` methods.
   Therefore no cycles of shared pointers are possible. 

Notes:
1. For some reason, we do not use `spdlog`'s API for logger creation. Instead, we create loggers manually.
   Normally, the process is: "hey `spdlog`, give me a logger" "sure, here's a shared pointer to it", where
   `spdlog` handles the creation and registration of (optionally thread-safe) loggers.
   Instead, we do: "we're manually creating a `spdlog` logger. we're manually registering a `spdlog` logger."
   The `spdlog` registry is only used for flushing every logger with `spdlog::flush_every`.
2. I am not convinced that our manual creation of `spdlog::logger` instances will produce thread-safe logging,
   but since we haven't seen issues so far, I guess it is fine for now.  
