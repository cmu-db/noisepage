// Terrier port
SETTING_int(port, "Terrier port (default: 15721)", 15721, 1024, 65535, false, terrier::settings::Callbacks::NoOp)

// Buffer pool size in transaction manager
SETTING_int(buffer_pool_size, "Buffer pool size in transaction manager (default : 100000)", 100000, 1, 1000000,
            true, terrier::settings::Callbacks::BufferSegmentPoolSizeLimit)

// If parallel execution is enabled
SETTING_bool(parallel_execution, "Enable parallel execution of queries (default: true)", true, true,
             terrier::settings::Callbacks::NoOp)

//===----------------------------------------------------------------------===//
// Used only in Tests
//===----------------------------------------------------------------------===//

SETTING_int(fixed_int, "(Test only) A fixed int param", 100, 10, 1000, false, terrier::settings::Callbacks::NoOp)

SETTING_bool(fixed_bool, "(Test only) A fixed bool param", false, false, terrier::settings::Callbacks::NoOp)

SETTING_double(fixed_double, "(Test only) A fixed double param", 114.514, 100.0, 1000.0, false, terrier::settings::Callbacks::NoOp)

SETTING_string(fixed_string, "(Test only) A fixed string param", "You cannot change me", false, terrier::settings::Callbacks::NoOp)

// Test int knob
SETTING_int(lucky_number, "(Test only) Your lucky number", 114, 114, 514, true, terrier::settings::Callbacks::NoOp)

// Test string knob
SETTING_string(db_name, "(Test only) The name for this database", "Terrier", true, terrier::settings::Callbacks::NoOp)

// Test decimal knob
SETTING_double(pi, "(Test only) The value of pi", 3.14159, 3.0, 4.0, true, terrier::settings::Callbacks::NoOp)

// Test immutable boolean knob
SETTING_bool(p_eq_np, "(Test only) Whether P=NP", false, true, terrier::settings::Callbacks::NoOp)