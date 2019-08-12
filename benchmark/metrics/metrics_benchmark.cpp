#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"
#include "transaction/transaction_defs.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class MetricsBenchmark : public benchmark::Fixture {
 public:
  const uint32_t num_iters_ = 100000;
  const uint32_t num_threads_ = 4;

  metrics::MetricsManager *metrics_manager_;

  void SetUp(const benchmark::State &state) final { metrics_manager_ = new metrics::MetricsManager; }

  void TearDown(const benchmark::State &state) final { delete metrics_manager_; }
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

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, RecordScalability)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    auto workload1 = [&](uint32_t id) {
      metrics_manager_->RegisterThread();

      for (uint32_t i = 0; i < num_iters_; i++) {
        common::thread_context.metrics_store_->RecordBeginData(i, transaction::timestamp_t(i));
        common::thread_context.metrics_store_->RecordCommitData(i, transaction::timestamp_t(i));
      }
    };

    common::WorkerPool thread_pool(num_threads_, {});
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> tested_timer(&elapsed_ms);
      for (uint32_t j = 0; j < thread_pool.NumWorkers(); j++) {
        thread_pool.SubmitTask([j, &workload1] { workload1(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }

    auto workload2 = [&](uint32_t id) { metrics_manager_->UnregisterThread(); };
    for (uint32_t j = 0; j < thread_pool.NumWorkers(); j++) {
      thread_pool.SubmitTask([j, &workload2] { workload2(j); });
    }
    thread_pool.WaitUntilAllFinished();

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    metrics_manager_->DisableMetric(metrics::MetricsComponent::TRANSACTION);
  }

  state.SetItemsProcessed(state.iterations() * num_iters_ * num_threads_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricsBenchmark, AggregateScalability)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    auto workload1 = [&](uint32_t id) {
      metrics_manager_->RegisterThread();

      for (uint32_t i = 0; i < num_iters_; i++) {
        common::thread_context.metrics_store_->RecordBeginData(i, transaction::timestamp_t(i));
        common::thread_context.metrics_store_->RecordCommitData(i, transaction::timestamp_t(i));
      }
    };

    common::WorkerPool thread_pool(num_threads_, {});
    for (uint32_t j = 0; j < thread_pool.NumWorkers(); j++) {
      thread_pool.SubmitTask([j, &workload1] { workload1(j); });
    }
    thread_pool.WaitUntilAllFinished();

    uint64_t elapsed_ns;
    {
      common::ScopedTimer<std::chrono::nanoseconds> tested_timer(&elapsed_ns);
      metrics_manager_->Aggregate();
    }

    auto workload2 = [&](uint32_t id) { metrics_manager_->UnregisterThread(); };
    for (uint32_t j = 0; j < thread_pool.NumWorkers(); j++) {
      thread_pool.SubmitTask([j, &workload2] { workload2(j); });
    }
    thread_pool.WaitUntilAllFinished();

    state.SetIterationTime(static_cast<double>(elapsed_ns) / 1000000000.0);
    metrics_manager_->DisableMetric(metrics::MetricsComponent::TRANSACTION);
  }

  state.SetItemsProcessed(state.iterations() * num_iters_ * num_threads_);
}

// BENCHMARK_REGISTER_F(MetricsBenchmark,
// RecordScalability)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);
// BENCHMARK_REGISTER_F(MetricsBenchmark, AggregateScalability)
//    ->Unit(benchmark::kNanosecond)
//    ->UseManualTime()
//    ->Iterations(10);

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
