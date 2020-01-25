#pragma once

#include <cstdint>

namespace terrier {
/**
 * This string specifies the environment variable that we will use to set the
 * number of threads to use for benchmarks.
 */
constexpr char ENV_NUM_THREADS[] = "TERRIER_BENCHMARK_THREADS";

/**
 * This class is a placeholder for any global configuration parameters for benchmarks.
 * You have to define a parameter as a static data member in order to avoid linker errors.
 * Be sure to also instantiate the parameter in the CPP file otherwise it will go undefined.
 */
class BenchmarkConfig {
 public:
  /**
   * This is a global variable that we use to specify the number of threads to use
   * for a microbenchmark. The default value is '1' but you can override it with
   * an environment variable.
   * @see terrier::ENV_NUM_THREADS
   */
  static uint32_t num_threads;
};

}  // namespace terrier
