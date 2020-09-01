#include "benchmark_util/benchmark_config.h"

namespace terrier {

// Instantiating the static variables in BenchmarkConfig here so that we don't have linker errors
uint32_t BenchmarkConfig::num_threads = 1;
uint32_t BenchmarkConfig::num_daf_threads = 1;
std::string_view BenchmarkConfig::logfile_path = "/tmp/benchmark.log";

}  // namespace terrier
