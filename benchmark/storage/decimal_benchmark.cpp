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
      noisepage::execution::sql::Decimal128 d(i);
      decimals_.push_back(d);
      float f = (static_cast<float>(i)) / (static_cast<float>(1000));
      floats_.push_back(f);
    }
  }

  void TearDown(const benchmark::State &state) final {}

  std::vector<noisepage::execution::sql::Decimal128> decimals_;
  std::vector<float> floats_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DecimalBenchmark, AddDecimal)(benchmark::State &state) {
  uint64_t elapsed_ms;
  {
    noisepage::execution::sql::Decimal128 result(0);
    noisepage::common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
    for (unsigned j = 0; j < 1000; j++)
      for (auto &decimal : decimals_) {
        result += decimal;
      }
  }
  state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DecimalBenchmark, AddFloat)(benchmark::State &state) {
  uint64_t elapsed_ms;
  {
    float result = 0;
    noisepage::common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
    for (unsigned j = 0; j < 1000; j++)
      for (auto &fl : floats_) {
        result += fl;
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
