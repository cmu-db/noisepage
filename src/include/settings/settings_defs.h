// SETTING_<type>(name, description, default_value, min_value, max_value, is_mutable, callback_fn)

// Terrier port
SETTING_int(port, "Terrier port (default: 15721)", 15721, 1024, 65535, false, terrier::settings::Callbacks::NoOp)

// RecordBufferSegmentPool size limit
SETTING_int(record_buffer_segment_size, "The maximum number of record buffer segments in the system. (default: 100000)",
    100000, 1, 1000000, true, terrier::settings::Callbacks::BufferSegmentPoolSizeLimit)

// RecordBufferSegmentPool reuse limit
SETTING_int(record_buffer_segment_reuse,
    "The minimum number of record buffer segments to keep allocated in the system (default: 1000)", 1000, 1, 1000000,
    true, terrier::settings::Callbacks::BufferSegmentPoolReuseLimit)

// If parallel execution is enabled
SETTING_bool(parallel_execution, "Enable parallel execution of queries (default: true)", true, true,
             terrier::settings::Callbacks::NoOp)

// Garbage collector thread interval
SETTING_int(gc_interval, "Garbage collector thread interval (default: 10)", 10, 1, 10000, false,
    terrier::settings::Callbacks::NoOp)

// Number of worker pool threads
SETTING_int(num_worker_threads, "The number of worker pool threads (default: 4)", 4, 1, 1000, true,
    terrier::settings::Callbacks::WorkerPoolThreads)

//===----------------------------------------------------------------------===//
// Used only in Tests
//===----------------------------------------------------------------------===//

#ifndef NDEBUG

SETTING_int(fixed_int, "(Test only) A fixed int param", 100, 10, 1000, false, terrier::settings::Callbacks::NoOp)

SETTING_bool(fixed_bool, "(Test only) A fixed bool param", false, false, terrier::settings::Callbacks::NoOp)

SETTING_double(fixed_double, "(Test only) A fixed double param", 114.514, 100.0, 1000.0, false,
    terrier::settings::Callbacks::NoOp)

SETTING_string(fixed_string, "(Test only) A fixed string param", "You cannot change me", false,
    terrier::settings::Callbacks::NoOp)

// Test int knob
SETTING_int(lucky_number, "(Test only) Your lucky number", 114, 114, 514, true, terrier::settings::Callbacks::NoOp)

// Test string knob
SETTING_string(db_name, "(Test only) The name for this database", "Terrier", true, terrier::settings::Callbacks::NoOp)

// Test decimal knob
SETTING_double(pi, "(Test only) The value of pi", 3.14159, 3.0, 4.0, true, terrier::settings::Callbacks::NoOp)

// Test immutable boolean knob
SETTING_bool(p_eq_np, "(Test only) Whether P=NP", false, true, terrier::settings::Callbacks::NoOp)

#endif