//===----------------------------------------------------------------------===//
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//

// Terrier port
SETTING_int(port, "Terrier port (default: 15721)", 15721, 1024, 65535, false, MainDatabase::EmptyCallback)  // NOLINT

    // Maximum number of connections
    SETTING_int(max_connections, "Maximum number of connections (default: 64)", 64, 1, 512, true,
                MainDatabase::EmptyCallback)
    // Buffer pool size in transaction manager
    SETTING_int(buffer_pool_size, "Buffer pool size in transaction manager (default : 100000)", 100000, 1, 1000000,
                true, MainDatabase::BufferPoolSizeCallback)
    // RPC port
    SETTING_int(rpc_port, "Terrier rpc port (default: 15445)", 15445, 1024, 65535, false, MainDatabase::EmptyCallback)

    // TODO(tianyu): Remove when we change to a different rpc framework
    // This is here only because capnp cannot exit gracefully and thus causes
    // test failure. This is an issue with the capnp implementation and has
    // been such way for a while, so it's unlikely it gets fixed.
    // See: https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
    SETTING_bool(rpc_enabled, "Enable rpc, this should be turned off when testing", false, false)

    /*
    // Socket family
    SETTING_string(socket_family,
    "Socket family (default: AF_INET)",
    "AF_INET",
    false, false)

    // Added for SSL only begins

    // Enables SSL connection. The default value is false
    SETTING_bool(ssl, "Enable SSL connection (default: true)", true, false, false)

    // Terrier private key file
    // Currently use hardcoded private key path, may need to change
    // to generate file dynamically at runtime
    // The same applies to certificate file
    SETTING_string(private_key_file,
    "path to private key file",
    "terrier_insecure_server.key",
    false, false)

    // Terrier certificate file
    SETTING_string(certificate_file,
    "path to certificate file",
    "terrier_insecure_server.crt",
    false, false)

    // Terrier root certificate file
    SETTING_string(root_cert_file,
    "path to root certificate file",
    "root.crt",
    false, false)
    */

    //===----------------------------------------------------------------------===//
    // RESOURCE USAGE
    //===----------------------------------------------------------------------===//

    SETTING_double(bnlj_buffer_size, "The default buffer size to use for blockwise nested loop joins (default: 1 MB)",
                   1.0 * 1024.0 * 1024.0, 1.0 * 1024, 1.0 * 1024.0 * 1024.0 * 1024, true)

    // Size of the MonoQueue task queue
    SETTING_int(monoqueue_task_queue_size, "MonoQueue Task Queue Size (default: 32)", 32, 8, 128, false,
                MainDatabase::EmptyCallback)

    // Size of the MonoQueue worker pool
    SETTING_int(monoqueue_worker_pool_size, "MonoQueue Worker Pool Size (default: 4)", 4, 1, 32, false,
                MainDatabase::EmptyCallback)

    // Number of connection threads used by terrier
    SETTING_int(connection_thread_count, "Number of connection threads (default: std::hardware_concurrency())",
                std::thread::hardware_concurrency(), 1, 64, false, MainDatabase::EmptyCallback)

    // Number of gc_threads
    SETTING_int(gc_num_threads, "The number of Garbage collection threads to run", 1, 1, 128, true,
                MainDatabase::EmptyCallback)

    // If parallel execution is enabled
    SETTING_bool(parallel_execution, "Enable parallel execution of queries (default: true)", true, true)

    // Minimum number of tuples a table must have
    SETTING_int(min_parallel_table_scan_size,                                                               // NOLINT
                "Minimum number of tuples a table must have before we consider performing parallel scans "  // NOLINT
                "(default: 10K)",                                                                           // NOLINT
                10 * 1000, 1, std::numeric_limits<int32_t>::max(), true, MainDatabase::EmptyCallback)       // NOLINT

    //===----------------------------------------------------------------------===//
    // WRITE AHEAD LOG
    //===----------------------------------------------------------------------===//

    //===----------------------------------------------------------------------===//
    // ERROR REPORTING AND LOGGING
    //===----------------------------------------------------------------------===//

    //===----------------------------------------------------------------------===//
    // STATISTICS
    //===----------------------------------------------------------------------===//

    // Enable or disable statistics collection
    /*
    SETTING_int(stats_mode,
    "Enable statistics collection (default: 0)",
    static_cast<int>(terrier::StatsType::INVALID),
    0, 16,
    true, true)
    */

    //===----------------------------------------------------------------------===//
    // AI
    //===----------------------------------------------------------------------===//

    // Enable or disable index tuner
    SETTING_bool(index_tuner, "Enable index tuner (default: false)", false, true)

    // Enable or disable layout tuner
    SETTING_bool(layout_tuner, "Enable layout tuner (default: false)", false, true)

    //===----------------------------------------------------------------------===//
    // BRAIN
    //===----------------------------------------------------------------------===//

    // Enable or disable brain
    SETTING_bool(brain, "Enable brain (default: false)", false, true)

    /*
    SETTING_string(terrier_address,
    "ip and port of the terrier rpc service, address:port",
    "127.0.0.1:15445",
    false, false)
    */

    // Size of the brain task queue
    SETTING_int(brain_task_queue_size, "Brain Task Queue Size (default: 32)", 32, 1, 128, false,
                MainDatabase::EmptyCallback)

    // Size of the brain worker pool
    SETTING_int(brain_worker_pool_size, "Brain Worker Pool Size (default: 1)", 1, 1, 16, false,
                MainDatabase::EmptyCallback)

    //===----------------------------------------------------------------------===//
    // CODEGEN
    //===----------------------------------------------------------------------===//

    // If code-generation is enabled
    SETTING_bool(codegen, "Enable code-generation for query execution (default: true)", true, true)

    // If llvm code is interpreted
    SETTING_bool(codegen_interpreter, "Force interpretation of generated llvm code (default: false)", false, true)

    // If statics will be printed on generated IR
    SETTING_bool(print_ir_stats, "Print statistics on generated IR (default: false)", false, true)

    // If logging of all generated IR is enabled
    SETTING_bool(dump_ir, "Enable logging of all generated IR (default: false)", false, true)

    //===----------------------------------------------------------------------===//
    // Optimizer
    //===----------------------------------------------------------------------===//
    SETTING_bool(predicate_push_down, "Enable predicate push-down optimization (default: true)", true, true)

    // If bloom filter for hash join in codegen is enabled
    SETTING_bool(hash_join_bloom_filter, "Enable bloom filter for hash join in codegen (default: false)", false, true)
    // Timeout for task execution
    SETTING_int(task_execution_timeout,                                // NOLINT
                "Maximum allowed length of time (in ms) for task "     // NOLINT
                "execution step of optimizer, "                        // NOLINT
                "assuming one plan has been found (default 5000)",     // NOLINT
                5000, 1000, 60000, true, MainDatabase::EmptyCallback)  // NOLINT

    //===----------------------------------------------------------------------===//
    // GENERAL
    //===----------------------------------------------------------------------===//

    // Display configuration
    SETTING_bool(display_settings, "Display settings (default: false)", false, true)
