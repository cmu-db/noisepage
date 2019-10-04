#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <random>

#include "benchmark/benchmark.h"
#include "common/perf_monitor.h"
#include "common/scoped_timer.h"
#include "common/thread_cpu_timer.h"

namespace terrier {

class DiskBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    file_ = open(path_, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    TERRIER_ASSERT(file_ >= 0, "File open failed.");

    int ret UNUSED_ATTRIBUTE = ftruncate(file_, NUM_BYTES);
    TERRIER_ASSERT(ret == 0, "File truncate failed.");

    ssize_t bytes_written = 0;
    const char buffer[8] = "terrier";
    while (static_cast<size_t>(bytes_written) < NUM_BYTES) {
      ssize_t bytes = write(file_, buffer, 8);
      TERRIER_ASSERT(bytes == 8, "File write failed.");
      bytes_written += bytes;
    }

    fsync(file_);

    ret = close(file_);
    TERRIER_ASSERT(ret == 0, "File close failed.");
  }

  void TearDown(const benchmark::State &state) final {}

  // Workload
  const char *const path_ = "./test_file.txt";
  static constexpr uint32_t NUM_BYTES = 1 << 20;
  static constexpr uint32_t OPERATION_SIZE = 1 << 3;
  static constexpr uint32_t NUM_OPERATIONS = NUM_BYTES / OPERATION_SIZE;
  static_assert(NUM_BYTES % OPERATION_SIZE == 0, "num_bytes_ must be a multiple of operation_size_.");

  // Test infrastructure
  int file_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskBenchmark, SequentialRead)(benchmark::State &state) {
  uint64_t blocks_read = 0;
  uint64_t blocks_written = 0;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::PerfMonitor monitor;
    common::ThreadUsage usage;
    usage.Start();
    monitor.Start();
    char buffer[OPERATION_SIZE];
    file_ = open(path_, O_RDONLY);
    TERRIER_ASSERT(file_ >= 0, "File open failed.");
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // workload

      ssize_t bytes_read = 0;
      while (static_cast<size_t>(bytes_read) < NUM_BYTES) {
        ssize_t bytes = read(file_, buffer, OPERATION_SIZE);
        TERRIER_ASSERT(bytes == OPERATION_SIZE, "File read failed.");
        bytes_read += bytes;
      }
    }
    auto ret UNUSED_ATTRIBUTE = close(file_);
    TERRIER_ASSERT(ret == 0, "File close failed.");
    usage.Stop();
    monitor.Stop();

    const auto counters = monitor.ReadCounters();
    std::cout << "CPU cycles: " << counters.cpu_cycles_ << std::endl;
    std::cout << "Instructions: " << counters.instructions_ << std::endl;
    std::cout << "Cache references: " << counters.cache_references_ << std::endl;
    std::cout << "Cache misses: " << counters.cache_misses_ << std::endl;
    std::cout << "Branch instructions" << counters.branch_instructions_ << std::endl;
    std::cout << "Branch misses: " << counters.branch_misses_ << std::endl;
    std::cout << "Bus cycles:: " << counters.bus_cycles_ << std::endl;
    std::cout << "Reference CPU cycles: " << counters.ref_cpu_cycles_ << std::endl << std::endl;

    std::cout << "User time: " << usage.ElapsedCPUTime().user_time_us_ << std::endl;
    std::cout << "Kernel time: " << usage.ElapsedCPUTime().system_time_us_ << std::endl << std::endl;

    blocks_read += usage.ElapsedDiskBlocks().read_blocks_;
    blocks_written += usage.ElapsedDiskBlocks().write_blocks_;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.counters["Blocks read"] = blocks_read / state.iterations();
  state.counters["Blocks written"] = blocks_written / state.iterations();
  state.SetItemsProcessed(state.iterations() * NUM_OPERATIONS);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskBenchmark, RandomRead)(benchmark::State &state) {
  std::vector<int64_t> byte_permutation;
  std::default_random_engine generator;

  byte_permutation.reserve(NUM_BYTES / OPERATION_SIZE);
  for (uint32_t i = 0; i < NUM_BYTES; i += OPERATION_SIZE) {
    byte_permutation.emplace_back(i);
  }
  std::shuffle(byte_permutation.begin(), byte_permutation.end(), generator);

  uint64_t blocks_read = 0;
  uint64_t blocks_written = 0;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::PerfMonitor monitor;
    common::ThreadUsage usage;
    usage.Start();
    monitor.Start();
    char buffer[OPERATION_SIZE];
    file_ = open(path_, O_RDONLY);
    TERRIER_ASSERT(file_ >= 0, "File open failed.");
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // workload

      ssize_t bytes_read = 0;
      uint32_t i = 0;
      while (static_cast<size_t>(bytes_read) < NUM_BYTES) {
        const auto offset = byte_permutation[i];
        auto ret UNUSED_ATTRIBUTE = lseek(file_, offset, SEEK_SET);
        TERRIER_ASSERT(ret == offset, "File seek failed.");
        ssize_t bytes = read(file_, buffer, OPERATION_SIZE);
        TERRIER_ASSERT(bytes == OPERATION_SIZE, "File read failed.");
        bytes_read += bytes;
        i++;
      }
      TERRIER_ASSERT(i == byte_permutation.size(), "Didn't perform all of the seeks.");
    }
    auto ret UNUSED_ATTRIBUTE = close(file_);
    TERRIER_ASSERT(ret == 0, "File close failed.");
    usage.Stop();
    monitor.Stop();
    const auto counters = monitor.ReadCounters();
    std::cout << "CPU cycles: " << counters.cpu_cycles_ << std::endl;
    std::cout << "Instructions: " << counters.instructions_ << std::endl;
    std::cout << "Cache references: " << counters.cache_references_ << std::endl;
    std::cout << "Cache misses: " << counters.cache_misses_ << std::endl;
    std::cout << "Branch instructions" << counters.branch_instructions_ << std::endl;
    std::cout << "Branch misses: " << counters.branch_misses_ << std::endl;
    std::cout << "Bus cycles:: " << counters.bus_cycles_ << std::endl;
    std::cout << "Reference CPU cycles: " << counters.ref_cpu_cycles_ << std::endl << std::endl;

    std::cout << "User time: " << usage.ElapsedCPUTime().user_time_us_ << std::endl;
    std::cout << "Kernel time: " << usage.ElapsedCPUTime().system_time_us_ << std::endl << std::endl;

    blocks_read += usage.ElapsedDiskBlocks().read_blocks_;
    blocks_written += usage.ElapsedDiskBlocks().write_blocks_;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.counters["Blocks read"] = blocks_read / state.iterations();
  state.counters["Blocks written"] = blocks_written / state.iterations();
  state.SetItemsProcessed(state.iterations() * NUM_OPERATIONS);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskBenchmark, SequentialWrite)(benchmark::State &state) {
  uint64_t blocks_read = 0;
  uint64_t blocks_written = 0;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::PerfMonitor monitor;
    common::ThreadUsage usage;
    usage.Start();
    monitor.Start();
    char buffer[OPERATION_SIZE];
    file_ = open(path_, O_WRONLY);
    TERRIER_ASSERT(file_ >= 0, "File open failed.");
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // workload

      ssize_t bytes_written = 0;
      while (static_cast<size_t>(bytes_written) < NUM_BYTES) {
        ssize_t bytes = write(file_, buffer, OPERATION_SIZE);
        TERRIER_ASSERT(bytes == OPERATION_SIZE, "File read failed.");
        bytes_written += bytes;
      }
      fsync(file_);
    }
    auto ret UNUSED_ATTRIBUTE = close(file_);
    TERRIER_ASSERT(ret == 0, "File close failed.");
    usage.Stop();
    monitor.Stop();

    const auto counters = monitor.ReadCounters();
    std::cout << "CPU cycles: " << counters.cpu_cycles_ << std::endl;
    std::cout << "Instructions: " << counters.instructions_ << std::endl;
    std::cout << "Cache references: " << counters.cache_references_ << std::endl;
    std::cout << "Cache misses: " << counters.cache_misses_ << std::endl;
    std::cout << "Branch instructions" << counters.branch_instructions_ << std::endl;
    std::cout << "Branch misses: " << counters.branch_misses_ << std::endl;
    std::cout << "Bus cycles:: " << counters.bus_cycles_ << std::endl;
    std::cout << "Reference CPU cycles: " << counters.ref_cpu_cycles_ << std::endl << std::endl;

    std::cout << "User time: " << usage.ElapsedCPUTime().user_time_us_ << std::endl;
    std::cout << "Kernel time: " << usage.ElapsedCPUTime().system_time_us_ << std::endl << std::endl;

    blocks_read += usage.ElapsedDiskBlocks().read_blocks_;
    blocks_written += usage.ElapsedDiskBlocks().write_blocks_;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.counters["Blocks read"] = blocks_read / state.iterations();
  state.counters["Blocks written"] = blocks_written / state.iterations();
  state.SetItemsProcessed(state.iterations() * NUM_OPERATIONS);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskBenchmark, RandomWrite)(benchmark::State &state) {
  std::vector<int64_t> byte_permutation;
  std::default_random_engine generator;

  byte_permutation.reserve(NUM_BYTES / OPERATION_SIZE);
  for (uint32_t i = 0; i < NUM_BYTES; i += OPERATION_SIZE) {
    byte_permutation.emplace_back(i);
  }
  std::shuffle(byte_permutation.begin(), byte_permutation.end(), generator);

  uint64_t blocks_read = 0;
  uint64_t blocks_written = 0;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::PerfMonitor monitor;
    common::ThreadUsage usage;
    usage.Start();
    monitor.Start();
    char buffer[OPERATION_SIZE];
    file_ = open(path_, O_WRONLY);
    TERRIER_ASSERT(file_ >= 0, "File open failed.");
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // workload

      ssize_t bytes_written = 0;
      uint32_t i = 0;
      while (static_cast<size_t>(bytes_written) < NUM_BYTES) {
        const auto offset = byte_permutation[i];
        auto ret UNUSED_ATTRIBUTE = lseek(file_, offset, SEEK_SET);
        TERRIER_ASSERT(ret == offset, "File seek failed.");
        ssize_t bytes = write(file_, buffer, OPERATION_SIZE);
        TERRIER_ASSERT(bytes == OPERATION_SIZE, "File write failed.");
        bytes_written += bytes;
        i++;
      }
      TERRIER_ASSERT(i == byte_permutation.size(), "Didn't perform all of the seeks.");
      fsync(file_);
    }
    auto ret UNUSED_ATTRIBUTE = close(file_);
    TERRIER_ASSERT(ret == 0, "File close failed.");
    usage.Stop();
    monitor.Stop();

    const auto counters = monitor.ReadCounters();
    std::cout << "CPU cycles: " << counters.cpu_cycles_ << std::endl;
    std::cout << "Instructions: " << counters.instructions_ << std::endl;
    std::cout << "Cache references: " << counters.cache_references_ << std::endl;
    std::cout << "Cache misses: " << counters.cache_misses_ << std::endl;
    std::cout << "Branch instructions" << counters.branch_instructions_ << std::endl;
    std::cout << "Branch misses: " << counters.branch_misses_ << std::endl;
    std::cout << "Bus cycles:: " << counters.bus_cycles_ << std::endl;
    std::cout << "Reference CPU cycles: " << counters.ref_cpu_cycles_ << std::endl << std::endl;

    std::cout << "User time: " << usage.ElapsedCPUTime().user_time_us_ << std::endl;
    std::cout << "Kernel time: " << usage.ElapsedCPUTime().system_time_us_ << std::endl << std::endl;

    blocks_read += usage.ElapsedDiskBlocks().read_blocks_;
    blocks_written += usage.ElapsedDiskBlocks().write_blocks_;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.counters["Blocks read"] = blocks_read / state.iterations();
  state.counters["Blocks written"] = blocks_written / state.iterations();
  state.SetItemsProcessed(state.iterations() * NUM_OPERATIONS);
}

BENCHMARK_REGISTER_F(DiskBenchmark, SequentialRead)->Unit(benchmark::kMillisecond)->UseManualTime();
BENCHMARK_REGISTER_F(DiskBenchmark, RandomRead)->Unit(benchmark::kMillisecond)->UseManualTime();
BENCHMARK_REGISTER_F(DiskBenchmark, SequentialWrite)->Unit(benchmark::kMillisecond)->UseManualTime();
BENCHMARK_REGISTER_F(DiskBenchmark, RandomWrite)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace terrier
