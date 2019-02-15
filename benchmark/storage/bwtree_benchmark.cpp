#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "util/bwtree_test_util.h"
#include "util/multithread_test_util.h"

namespace terrier {

// Adapted from benchmarks in https://github.com/wangziqi2013/BwTree/blob/master/test/

class BwTreeBenchmark : public benchmark::Fixture {
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
  const uint32_t num_threads_ = 4;

  // Test infrastructure
  std::default_random_engine generator_;
  std::vector<int64_t> key_permutation_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, RandomInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const tree = BwTreeTestUtil::GetEmptyTree();

    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->Insert(key_permutation_[i], key_permutation_[i]);
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    delete tree;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsert)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const tree = BwTreeTestUtil::GetEmptyTree();

    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->Insert(i, i);
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    delete tree;
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, RandomInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
  for (uint32_t i = 0; i < num_keys_; i++) {
    tree->Insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->GetValue(key_permutation_[i], values);
        values.clear();
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete tree;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, RandomInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
  for (uint32_t i = 0; i < num_keys_; i++) {
    tree->Insert(key_permutation_[i], key_permutation_[i]);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->GetValue(i, values);
        values.clear();
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete tree;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsertRandomRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
  for (uint32_t i = 0; i < num_keys_; i++) {
    tree->Insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->GetValue(key_permutation_[i], values);
        values.clear();
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete tree;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BwTreeBenchmark, SequentialInsertSequentialRead)(benchmark::State &state) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
  for (uint32_t i = 0; i < num_keys_; i++) {
    tree->Insert(i, i);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      const uint32_t gcid = id + 1;
      tree->AssignGCID(gcid);

      uint32_t start_key = num_keys_ / num_threads_ * id;
      uint32_t end_key = start_key + num_keys_ / num_threads_;

      std::vector<int64_t> values;
      values.reserve(1);

      for (uint32_t i = start_key; i < end_key; i++) {
        tree->GetValue(i, values);
        values.clear();
      }
      tree->UnregisterThread(gcid);
    };

    uint64_t elapsed_ms;
    tree->UpdateThreadLocal(num_threads_ + 1);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    tree->UpdateThreadLocal(1);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  delete tree;
  state.SetItemsProcessed(state.iterations() * num_keys_);
}

BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);
BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsert)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);
BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BwTreeBenchmark, RandomInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsertRandomRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(BwTreeBenchmark, SequentialInsertSequentialRead)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
}  // namespace terrier
