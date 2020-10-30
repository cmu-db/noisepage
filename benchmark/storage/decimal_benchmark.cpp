#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/scoped_timer.h"
#include "execution/sql/runtime_types.h"

namespace terrier {

class DecimalBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random column of decimals and floats
    for (unsigned i = 0; i < 1000000; i++) {
      execution::sql::Decimal128 d(i);
      decimals_.push_back(d);
      float f = (static_cast<float>(i)) / (static_cast<float>(1000));
      floats_.push_back(f);
    }
  }

  void TearDown(const benchmark::State &state) final {}

  // Read buffers pointers for concurrent reads
  std::vector<execution::sql::Decimal128> decimals_;
  std::vector<int128_t> floats_;
};

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DecimalBenchmark, AddDecimal)(benchmark::State &state) {
  // NOLINTNEXTLINE
  uint64_t elapsed_ms;
  {
    execution::sql::Decimal128 result(0);
    common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
    for (unsigned j = 0; j < 1000; j++)
      for (unsigned i = 0; i < decimals_.size(); i++) {
        result += decimals_[i];
      }
  }
  state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DecimalBenchmark, AddFloat)(benchmark::State &state) {
  // NOLINTNEXTLINE
  uint64_t elapsed_ms;
  {
    float result = 0;
    common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
    for (unsigned j = 0; j < 1000; j++)
      for (unsigned i = 0; i < floats_.size(); i++) {
        result += floats_[i];
      }
  }
  state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
}

// ----------------------------------------------------------------------------
// Benchmark Registration
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(DecimalBenchmark, AddDecimal)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DecimalBenchmark, AddFloat)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
// clang-format on

}  // namespace terrier
