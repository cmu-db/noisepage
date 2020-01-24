#include "benchmark_util/benchmark_config.h"

namespace terrier {

// Instantiating the static variables in BenchmarkConfig here so that we don't have linker errors
uint32_t BenchmarkConfig::num_threads = 1;

}  // namespace terrier
