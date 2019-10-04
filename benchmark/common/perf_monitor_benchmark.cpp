#include "benchmark/benchmark.h"
#include "common/perf_monitor.h"
#include "common/big_perf_monitor.h"

namespace terrier {

class CuckooMapBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {

  }

  void TearDown(const benchmark::State &state) final {}
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const index = new CuckooMap(256);

    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      for (uint32_t i = start_key; i < end_key; i++) {
        index->insert(key_permutation_[i], key_permutation_[i]);
      }
    };

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    delete index;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const index = new CuckooMap(256);

    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      for (uint32_t i = start_key; i < end_key; i++) {
        index->insert(i, i);
      }
    };

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    delete index;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

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
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, RandomInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

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
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

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
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CuckooMapBenchmark, SequentialInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const index = new CuckooMap(256);
  for (uint32_t i = 0; i < num_keys_; i++) {
    index->insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

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
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete index;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

BENCHMARK_REGISTER_F(CuckooMapBenchmark, RandomInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);
BENCHMARK_REGISTER_F(CuckooMapBenchmark, SequentialInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);
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
}  // namespace terrier
