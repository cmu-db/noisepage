#include <common/scoped_timer.h>

#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/macros.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"

namespace terrier {

class ExecutionThreadPoolBenchmark : public benchmark::Fixture {
 public:
  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_}};
  const uint32_t initial_table_size_ = 100000;
  const uint32_t tables_per_region_ = 2 * std::thread::hardware_concurrency();
  const uint32_t iterators_per_table_ = 20;
  storage::BlockStore block_store_{100000000, 100000000};
  storage::RecordBufferSegmentPool buffer_pool_{100000000, 100000000};
  std::default_random_engine generator_;
  const storage::ProjectedRowInitializer initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  storage::ProjectedRow *redo_;
  byte *redo_buffer_;

  static std::vector<int> GetNCPUsInFewRegions(int n) {
    std::vector<int> result;
    for (int region = 0;
         region < static_cast<int>(storage::RawBlock::GetNumNumaRegions()) && static_cast<int>(result.size()) < n;
         region++) {
      for (int cpu = 0;
           cpu < static_cast<int>(std::thread::hardware_concurrency()) && static_cast<int>(result.size()) < n; cpu++) {
#ifdef __APPLE__
        result.emplace_back(cpu);  // NOLINT
#else
        if (numa_available() != -1 && numa_node_of_cpu(cpu) == region) result.emplace_back(cpu);
#endif
      }
    }
    return result;
  }

  static std::vector<int> GetOneCPUPerRegion() {
    std::vector<int> result;
    for (int region = 0; region < static_cast<int>(storage::RawBlock::GetNumNumaRegions()); region++) {
      for (int cpu = 0; cpu < static_cast<int>(std::thread::hardware_concurrency()); cpu++) {
#ifdef __APPLE__
        result.emplace_back(cpu);
        break;
#else
        if (numa_available() != -1 && numa_node_of_cpu(cpu) == region) {
          result.emplace_back(cpu);
          break;
        }
#endif
      }
    }
    return result;
  }

  void FillTables(std::vector<std::vector<storage::DataTable *>> &tables) {
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    TERRIER_ASSERT(tables.empty(), "tables should be empty");
    for (uint32_t i = 0; i < static_cast<uint32_t>(storage::RawBlock::GetNumNumaRegions()); i++) {
      tables.emplace_back(std::vector<storage::DataTable *>(tables_per_region_));
    }

    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids = GetOneCPUPerRegion();

    TERRIER_ASSERT(cpu_ids.size() == storage::RawBlock::GetNumNumaRegions(), "should actually get right number of regions");

    for (uint32_t i = 0; i < static_cast<uint32_t>(storage::RawBlock::GetNumNumaRegions()); i++) {
      std::vector<int> single_id;
      single_id.emplace_back(cpu_ids[i]);
      common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &single_id);

      std::promise<void> p;
      thread_pool.SubmitTask(&p, [&] {
        transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                            common::ManagedPointer(&buffer_pool_), DISABLED);
        for (uint32_t j = 0; j < tables[i].size(); j++) {
          tables[i][j] = new storage::DataTable(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                        storage::layout_version_t(0));
          for (uint32_t tuple_num = 0; tuple_num < initial_table_size_; tuple_num++) {
            tables[i][j]->Insert(common::ManagedPointer(&txn), *redo_);
          }
        }
      });

      p.get_future().get();
    }

    delete[] redo_buffer_;
  }
};


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionThreadPoolBenchmark, ConcurrentWorkload)(benchmark::State &state) {
  std::vector<std::vector<storage::DataTable *>> tables;
  FillTables(tables);

  for (auto _ : state) {
    std::vector<std::function<void()>> lambdas;
    common::SharedLatch latch;

    for (auto &v : tables) {
      for (storage::DataTable *table : v) {
        for (uint32_t UNUSED_ATTRIBUTE i = 0; i < iterators_per_table_; i++) {
          lambdas.emplace_back([&, table] {
            common::SharedLatch::ScopedSharedLatch l(&latch);
            uint32_t count = 0;
            for (auto it = table->begin(); it != table->end(); it++) count++;
            TERRIER_ASSERT(count == initial_table_size_, "should see the right number of tuples per table");
          });
        }
      }
    }

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      std::thread *threads[lambdas.size()];
      latch.LockExclusive();
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        threads[i] = new std::thread(lambdas[i]);
      }
      latch.Unlock();

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        threads[i]->join();
        delete threads[i];
      }
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * initial_table_size_ * iterators_per_table_ * tables_per_region_ * tables.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionThreadPoolBenchmark, ConcurrentThreadPoolWorkload)(benchmark::State &state) {
  std::vector<std::vector<storage::DataTable *>> tables;
  FillTables(tables);

  for (auto _ : state) {
    std::vector<std::function<void()>> lambdas;
    common::SharedLatch latch;

    for (auto &v : tables) {
      for (storage::DataTable *table : v) {
        for (uint32_t UNUSED_ATTRIBUTE i = 0; i < iterators_per_table_; i++) {
          lambdas.emplace_back([&, table] {
            common::SharedLatch::ScopedSharedLatch l(&latch);
            uint32_t count = 0;
            for (auto it = table->begin(); it != table->end(); it++) count++;
            TERRIER_ASSERT(count == initial_table_size_, "should see the right number of tuples per table");
          });
        }
      }
    }

    std::promise<void> promises[lambdas.size()];
    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids(std::thread::hardware_concurrency());
    for (uint32_t i = 0; i < cpu_ids.size(); i++) cpu_ids[i] = i;
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      latch.LockExclusive();
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i]);
      }
      latch.Unlock();

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * initial_table_size_ * iterators_per_table_ * tables_per_region_ * tables.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionThreadPoolBenchmark, ConcurrentNUMAThreadPoolWorkload)(benchmark::State &state) {
  std::vector<std::vector<storage::DataTable *>> tables;
  FillTables(tables);

  for (auto _ : state) {
    std::vector<std::pair<std::function<void()>, common::numa_region_t>> lambdas;
    common::SharedLatch latch;

    for (int16_t region = 0; region < static_cast<int16_t>(tables.size()); region++) {
      for (storage::DataTable *table : tables[region]) {
        for (uint32_t UNUSED_ATTRIBUTE i = 0; i < iterators_per_table_; i++) {
          lambdas.emplace_back([&, table] {
            common::SharedLatch::ScopedSharedLatch l(&latch);
            uint32_t count = 0;
            for (auto it = table->begin(); it != table->end(); it++) count++;
            TERRIER_ASSERT(count == initial_table_size_, "should see the right number of tuples per table");
          }, static_cast<common::numa_region_t>(region));
        }
      }
    }

    std::promise<void> promises[lambdas.size()];
    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids(std::thread::hardware_concurrency());
    for (uint32_t i = 0; i < cpu_ids.size(); i++) cpu_ids[i] = i;
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      latch.LockExclusive();
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i].first, lambdas[i].second);
      }
      latch.Unlock();

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * initial_table_size_ * iterators_per_table_ * tables_per_region_ * tables.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionThreadPoolBenchmark, ConcurrentThreadPoolWithYieldingWorkload)(benchmark::State &state) {
  std::vector<std::vector<storage::DataTable *>> tables;
  FillTables(tables);

  for (auto _ : state) {
    std::vector<std::pair<std::function<void(common::PoolContext*)>, common::numa_region_t>> lambdas;
    common::SharedLatch latch;

    for (int16_t region = 0; region < static_cast<int16_t>(tables.size()); region++) {
      for (storage::DataTable *table : tables[region]) {
        for (uint32_t UNUSED_ATTRIBUTE i = 0; i < iterators_per_table_; i++) {
          lambdas.emplace_back([&, table] (common::PoolContext *ctx) {
            common::SharedLatch::ScopedSharedLatch l(&latch, ctx);
            uint32_t count = 0;
            for (auto it = table->begin(ctx); it != table->end(ctx); it++) count++;
            TERRIER_ASSERT(count == initial_table_size_, "should see the right number of tuples per table");
          }, static_cast<common::numa_region_t>(region));
        }
      }
    }

    std::promise<void> promises[lambdas.size()];
    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids(std::thread::hardware_concurrency());
    for (uint32_t i = 0; i < cpu_ids.size(); i++) cpu_ids[i] = i;
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      latch.LockExclusive();
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i].first);
      }
      latch.Unlock();

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * initial_table_size_ * iterators_per_table_ * tables_per_region_ * tables.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionThreadPoolBenchmark, ConcurrentNUMAThreadPoolWithYieldingWorkload)(benchmark::State &state) {
  std::vector<std::vector<storage::DataTable *>> tables;
  FillTables(tables);

  for (auto _ : state) {
    std::vector<std::pair<std::function<void(common::PoolContext*)>, common::numa_region_t>> lambdas;
    common::SharedLatch latch;

    for (int16_t region = 0; region < static_cast<int16_t>(tables.size()); region++) {
      for (storage::DataTable *table : tables[region]) {
        for (uint32_t UNUSED_ATTRIBUTE i = 0; i < iterators_per_table_; i++) {
          lambdas.emplace_back([&, table] (common::PoolContext *ctx) {
            common::SharedLatch::ScopedSharedLatch l(&latch, ctx);
            uint32_t count = 0;
            for (auto it = table->begin(ctx); it != table->end(ctx); it++) count++;
            TERRIER_ASSERT(count == initial_table_size_, "should see the right number of tuples per table");
          }, static_cast<common::numa_region_t>(region));
        }
      }
    }

    std::promise<void> promises[lambdas.size()];
    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids(std::thread::hardware_concurrency());
    for (uint32_t i = 0; i < cpu_ids.size(); i++) cpu_ids[i] = i;
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      latch.LockExclusive();
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i].first, lambdas[i].second);
      }
      latch.Unlock();

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * initial_table_size_ * iterators_per_table_ * tables_per_region_ * tables.size());
}

BENCHMARK_REGISTER_F(ExecutionThreadPoolBenchmark, ConcurrentWorkload)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(ExecutionThreadPoolBenchmark, ConcurrentThreadPoolWorkload)
->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(ExecutionThreadPoolBenchmark, ConcurrentNUMAThreadPoolWorkload)
->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(ExecutionThreadPoolBenchmark, ConcurrentThreadPoolWithYieldingWorkload)
->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(ExecutionThreadPoolBenchmark, ConcurrentNUMAThreadPoolWithYieldingWorkload)
->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
}  // namespace terrier
