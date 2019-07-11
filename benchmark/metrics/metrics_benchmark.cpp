#include <iostream>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector_thread.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class MetricsBenchmark : public benchmark::Fixture {
 public:
  const uint32_t num_iters_ = 10000000;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, AlwaysTimerNoConditional)(benchmark::State &state) {
  std::vector<bool> do_it(num_iters_, true);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<bool> did_it(num_iters_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_iters_; i++) {
        uint64_t tested_ns;
        {
          common::ScopedTimer<std::chrono::nanoseconds> tested_timer(&tested_ns);
          did_it[i] = true;
        }
      }
    }
    EXPECT_EQ(num_iters_, std::count(did_it.cbegin(), did_it.cend(), true));
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_iters_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, AlwaysTimerConditional)(benchmark::State &state) {
  std::vector<bool> do_it(num_iters_, true);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<bool> did_it(num_iters_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_iters_; i++) {
        if (do_it[i]) {
          uint64_t tested_ns;
          {
            common::ScopedTimer<std::chrono::nanoseconds> tested_timer(&tested_ns);
            did_it[i] = true;
          }
        } else {
          did_it[i] = true;
        }
      }
    }
    EXPECT_EQ(num_iters_, std::count(did_it.cbegin(), did_it.cend(), true));
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_iters_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, NeverTimerNoConditional)(benchmark::State &state) {
  std::vector<bool> do_it(num_iters_, true);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<bool> did_it(num_iters_, false);
    uint64_t elapsed_ns;
    {
      common::ScopedTimer<std::chrono::nanoseconds> timer(&elapsed_ns);
      for (uint32_t i = 0; i < num_iters_; i++) {
        did_it[i] = true;
      }
    }
    EXPECT_EQ(num_iters_, std::count(did_it.cbegin(), did_it.cend(), true));
    state.SetIterationTime(static_cast<double>(elapsed_ns) / 1000000000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_iters_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, NeverTimerConditional)(benchmark::State &state) {
  std::vector<bool> do_it(num_iters_, false);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<bool> did_it(num_iters_, false);
    uint64_t elapsed_ns;
    {
      common::ScopedTimer<std::chrono::nanoseconds> timer(&elapsed_ns);
      for (uint32_t i = 0; i < num_iters_; i++) {
        if (do_it[i]) {
          uint64_t tested_ns;
          {
            common::ScopedTimer<std::chrono::nanoseconds> tested_timer(&tested_ns);
            did_it[i] = true;
          }
        } else {
          did_it[i] = true;
        }
      }
    }
    EXPECT_EQ(num_iters_, std::count(did_it.cbegin(), did_it.cend(), true));
    state.SetIterationTime(static_cast<double>(elapsed_ns) / 1000000000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_iters_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, SometimesTimer)(benchmark::State &state) {
  std::bernoulli_distribution coin(0.5);
  std::default_random_engine generator;
  std::vector<bool> do_it(num_iters_);
  for (uint32_t i = 0; i < num_iters_; i++) {
    do_it[i] = coin(generator);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<bool> did_it(num_iters_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_iters_; i++) {
        if (do_it[i]) {
          uint64_t tested_ns;
          {
            common::ScopedTimer<std::chrono::nanoseconds> tested_timer(&tested_ns);
            did_it[i] = true;
          }
        } else {
          did_it[i] = true;
        }
      }
    }
    EXPECT_EQ(num_iters_, std::count(did_it.cbegin(), did_it.cend(), true));
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_iters_);
}

BENCHMARK_REGISTER_F(MetricsBenchmark, AlwaysTimerNoConditional)
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()
    ->Iterations(12);
BENCHMARK_REGISTER_F(MetricsBenchmark, AlwaysTimerConditional)
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()
    ->Iterations(12);
BENCHMARK_REGISTER_F(MetricsBenchmark, NeverTimerNoConditional)
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()
    ->Iterations(240);
BENCHMARK_REGISTER_F(MetricsBenchmark, NeverTimerConditional)
    ->Unit(benchmark::kMicrosecond)
    ->UseManualTime()
    ->Iterations(240);
BENCHMARK_REGISTER_F(MetricsBenchmark, SometimesTimer)->Unit(benchmark::kMicrosecond)->UseManualTime()->Iterations(24);

}  // namespace terrier
