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

// Garbage collector thread interval
SETTING_int(gc_interval, "Garbage collector thread interval (default: 10)", 10, 1, 10000, false,
    terrier::settings::Callbacks::NoOp)

// Number of worker pool threads
SETTING_int(num_worker_threads, "The number of worker pool threads (default: 4)", 4, 1, 1000, true,
    terrier::settings::Callbacks::WorkerPoolThreads)
