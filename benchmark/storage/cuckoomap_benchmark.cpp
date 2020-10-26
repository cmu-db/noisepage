#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/scoped_timer.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "test_util/multithread_test_util.h"
#include "xxHash/xxh3.h"

namespace noisepage {

/**
 * CuckooMap Benchmarks
 * Adapted from benchmarks in https://github.com/wangziqi2013/BwTree/blob/master/test/
 */
class CuckooMapBenchmark : public benchmark::Fixture {
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

  struct KeyHash {
    std::size_t operator()(const int64_t k) const {
      return XXH3_64bits(reinterpret_cast<const void *>(&(k)), sizeof(k));
    }
  };

  using CuckooMap = cuckoohash_map<int64_t, int64_t, KeyHash>;

  std::default_random_engine generator_;
  std::vector<int64_t> key_permutation_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const index = new CuckooMap(256);

    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      for (uint32_t i = start_key; i < end_key; i++) {
        index->insert(key_permutation_[i], key_permutation_[i]);
      }
    };

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
    }
    delete index;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const index = new CuckooMap(256);

    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      for (uint32_t i = start_key; i < end_key; i++) {
        index->insert(i, i);
      }
    };

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
    }
    delete index;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        values.emplace_back(index->find(key_permutation_[i]));
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

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        values.emplace_back(index->find(i));
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

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        values.emplace_back(index->find(key_permutation_[i]));
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

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / BenchmarkConfig::num_threads * id;
      uint32_t end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        values.emplace_back(index->find(i));
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

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(CuckooMapBenchmark, RandomInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, SequentialInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, RandomInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, RandomInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, SequentialInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, SequentialInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
// clang-format on

}  // namespace noisepage
