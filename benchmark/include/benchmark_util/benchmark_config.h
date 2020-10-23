#pragma once

#include <cstdint>
#include <string>

namespace noisepage {
/**
 * This string specifies the environment variable that we will use to set the
 * number of threads to use for benchmarks.
 */
constexpr char ENV_NUM_THREADS[] = "NOISEPAGE_BENCHMARK_THREADS";

/**
 * This string specifies the environment variable that we will use to set the
 * file path of the logfile for benchmarks.
 */
constexpr char ENV_LOGFILE_PATH[] = "NOISEPAGE_BENCHMARK_LOGFILE_PATH";

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
   * @see noisepage::ENV_NUM_THREADS
   */
  static uint32_t num_threads;

  /**
   * The path to use for the DBMS's WAL in benchmark runs.
   * The system assumes that the directory for the logfile is writable.
   * @see noisepage::ENV_LOGFILE_PATH
   */
  static std::string_view logfile_path;
};

}  // namespace noisepage
