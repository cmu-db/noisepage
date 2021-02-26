#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/scoped_timer.h"
#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"
#include "test_util/multithread_test_util.h"

namespace noisepage {

class BPlusTreeBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    key_permutation_.reserve(num_keys_);
    for (uint32_t i = 0; i < num_keys_; i++) {
      key_permutation_[i] = i;
    }
    std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);
  }

  void TearDown(const benchmark::State &state) final {}

  // Workload
  const uint32_t num_keys_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  std::vector<int64_t> key_permutation_;
  std::function<bool(const int64_t)> predicate_ = [](const int64_t slot) -> bool { return false; };
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, RandomInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    {
      auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();

      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        for (uint32_t i = start_key; i < end_key; i++) {
          storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
          p1.first = key_permutation_[i];
          p1.second = key_permutation_[i];
          tree->Insert(p1, predicate_);
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }

      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, SequentialInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    {
      auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();

      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        for (uint32_t i = start_key; i < end_key; i++) {
          storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
          p1.first = i;
          p1.second = i;
          tree->Insert(p1, predicate_);
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, RandomInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  {
    auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();
    for (uint32_t i = 0; i < num_keys_; i++) {
      storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = key_permutation_[i];
      p1.second = key_permutation_[i];
      tree->Insert(p1, predicate_);
    }

    // NOLINTNEXTLINE
    for (auto _ : state) {
      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        std::vector<int64_t> values;
        values.reserve(1);

        for (uint32_t i = start_key; i < end_key; i++) {
          tree->FindValueOfKey(key_permutation_[i], &values);
          values.clear();
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }

    state.SetItemsProcessed(state.iterations() * num_keys_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, RandomInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  {
    auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();
    for (uint32_t i = 0; i < num_keys_; i++) {
      storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = key_permutation_[i];
      p1.second = key_permutation_[i];
      tree->Insert(p1, predicate_);
    }

    // NOLINTNEXTLINE
    for (auto _ : state) {
      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        std::vector<int64_t> values;
        values.reserve(1);

        for (uint32_t i = start_key; i < end_key; i++) {
          tree->FindValueOfKey(i, &values);
          values.clear();
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }

    state.SetItemsProcessed(state.iterations() * num_keys_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, SequentialInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  {
    auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();
    for (uint32_t i = 0; i < num_keys_; i++) {
      storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = i;
      p1.second = i;
      tree->Insert(p1, predicate_);
    }

    // NOLINTNEXTLINE
    for (auto _ : state) {
      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        std::vector<int64_t> values;
        values.reserve(1);

        for (uint32_t i = start_key; i < end_key; i++) {
          tree->FindValueOfKey(key_permutation_[i], &values);
          values.clear();
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }

    state.SetItemsProcessed(state.iterations() * num_keys_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BPlusTreeBenchmark, SequentialInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  {
    auto tree = std::make_unique<storage::index::BPlusTree<int64_t, int64_t>>();
    for (uint32_t i = 0; i < num_keys_; i++) {
      storage::index::BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = i;
      p1.second = i;
      tree->Insert(p1, predicate_);
    }

    // NOLINTNEXTLINE
    for (auto _ : state) {
      auto workload = [&](uint32_t id) {
        uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
        uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

        std::vector<int64_t> values;
        values.reserve(1);

        for (uint32_t i = start_key; i < end_key; i++) {
          tree->FindValueOfKey(i, &values);
          values.clear();
        }
      };

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
      }
      state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    }

    state.SetItemsProcessed(state.iterations() * num_keys_);
  }
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, RandomInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, SequentialInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, RandomInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, RandomInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, SequentialInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BPlusTreeBenchmark, SequentialInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
// clang-format on

}  // namespace noisepage
