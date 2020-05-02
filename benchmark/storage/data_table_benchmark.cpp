#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/scoped_timer.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "transaction/transaction_context.h"

namespace terrier {

/**
 * This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
 * reference to other fast data structures (indexes) to compare against. We are interested in the DataTable's raw
 * performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
 * of the Google Tests.
 */
class DataTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    read_ = initializer_.InitializeRow(read_buffer_);

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < BenchmarkConfig::num_threads; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
      storage::ProjectedRow *read = initializer_.InitializeRow(read_buffer);
      read_buffers_.emplace_back(read_buffer);
      reads_.emplace_back(read);
    }
  }

  void FillAcrossNUMARegions(storage::DataTable *read_table) {
    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids = GetOneCPUPerRegion();
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry),
                                            &cpu_ids);
    std::promise<void> ps[cpu_ids.size()];
    for (int thread = 0; thread < static_cast<int>(cpu_ids.size()); thread++) {
      thread_pool.SubmitTask(&ps[thread], [&] {
        // populate read_table_ by inserting tuples
        // We can use dummy timestamps here since we're not invoking concurrency control
        transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                            common::ManagedPointer(&buffer_pool_), DISABLED);
        for (uint32_t i = 0; i < num_reads_ / cpu_ids.size(); ++i) {
          read_table->Insert(common::ManagedPointer(&txn), *redo_);
        }
      });
    }

    for (int thread = 0; thread < static_cast<int>(cpu_ids.size()); thread++) {
      ps[thread].get_future().wait();
    }
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    delete[] read_buffer_;
    for (uint32_t i = 0; i < BenchmarkConfig::num_threads; ++i) delete[] read_buffers_[i];
    // google benchmark might run benchmark several iterations. We need to clear vectors.
    read_buffers_.clear();
    reads_.clear();
  }

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

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));

  // Workload
  uint32_t num_inserts_ = atoi(std::getenv("NUM_OPS") == nullptr ? "1000000" : std::getenv("NUM_OPS"));
  uint32_t num_reads_ = atoi(std::getenv("NUM_OPS") == nullptr ? "1000000" : std::getenv("NUM_OPS"));
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{100000, 10000};
  storage::RecordBufferSegmentPool buffer_pool_{num_inserts_, buffer_pool_reuse_limit_};

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;

  // Read buffers pointers for concurrent reads
  std::vector<byte *> read_buffers_;
  std::vector<storage::ProjectedRow *> reads_;
};

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, Insert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                             storage::layout_version_t(0));
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                          common::ManagedPointer(&buffer_pool_), DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / BenchmarkConfig::num_threads; i++) {
        table.Insert(common::ManagedPointer(&txn), *redo_);
      }
    };
    common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
    thread_pool.Startup();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SelectRandom)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                      common::ManagedPointer(&buffer_pool_), DISABLED);
  std::vector<storage::TupleSlot> read_order;
  // adding num_reads_ rows in the table
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(common::ManagedPointer(&txn), *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < BenchmarkConfig::num_threads; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                          common::ManagedPointer(&buffer_pool_), DISABLED);
      for (uint32_t i = 0; i < num_reads_ / BenchmarkConfig::num_threads; i++)
        read_table.Select(common::ManagedPointer(&txn), read_order[(rand_read_offsets[id] + i) % read_order.size()],
                          reads_[id]);
    };
    common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
    thread_pool.Startup();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SelectSequential)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                      common::ManagedPointer(&buffer_pool_), DISABLED);
  std::vector<storage::TupleSlot> read_order;

  // inserted the table with 10 million rows
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(common::ManagedPointer(&txn), *redo_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                          common::ManagedPointer(&buffer_pool_), DISABLED);
      for (uint32_t i = 0; i < num_reads_ / BenchmarkConfig::num_threads; i++)
        read_table.Select(common::ManagedPointer(&txn), read_order[i], reads_[id]);
    };
    common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
    thread_pool.Startup();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, Scan)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                      common::ManagedPointer(&buffer_pool_), DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(common::ManagedPointer(&txn), *redo_));
  }

  std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(layout_);
  storage::ProjectedColumnsInitializer initializer(layout_, all_cols, common::Constants::K_DEFAULT_VECTOR_SIZE);

  std::vector<storage::ProjectedColumns *> all_columns;
  std::vector<byte *> buf;
  for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns = initializer.Initialize(buffer);
    all_columns.push_back(columns);
    buf.push_back(buffer);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      auto it = read_table.begin();
      while (it != read_table.end()) {
        read_table.Scan(common::ManagedPointer(&txn), &it, all_columns[id]);
      }
    };
    common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
    thread_pool.Startup();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  for (auto p : buf) {
    delete[] p;
  }
  state.SetItemsProcessed(state.iterations() * num_reads_ * BenchmarkConfig::num_threads);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SingleThreadedIteration)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  FillAcrossNUMARegions(&read_table);

  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids = GetNCPUsInFewRegions(1);
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t count = 0;
      for (auto _ UNUSED_ATTRIBUTE : read_table) {
        count++;
      }
    };
    std::promise<void> promises[1];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t j = 0; j < 1; j++) {
        thread_pool.SubmitTask(&promises[j], [j, &workload] { workload(j); });
      }

      // NOLINTNEXTLINE
      for (auto &promise : promises) {  // NOLINT
        promise.get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, NUMASingleThreadedIteration)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  FillAcrossNUMARegions(&read_table);

  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids = GetNCPUsInFewRegions(1);
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  std::vector<common::numa_region_t> numa_regions;
  read_table.GetNUMARegions(&numa_regions);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t count = 0;
      for (auto numa_region : numa_regions) {
        for (auto it = read_table.begin(numa_region); it != read_table.end(numa_region); it++) {
          count++;
        }
      }
    };

    std::promise<void> promises[numa_regions.size()];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t j = 0; j < 1; j++) {
        thread_pool.SubmitTask(&promises[j], [j, &workload] { workload(j); });
      }
      // NOLINTNEXTLINE
      for (uint32_t j = 0; j < 1; j++) {  // NOLINT
        promises[j].get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, NUMAMultiThreadedIteration)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  FillAcrossNUMARegions(&read_table);

  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids = GetNCPUsInFewRegions(storage::RawBlock::GetNumNumaRegions());
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  std::vector<common::numa_region_t> numa_regions;
  read_table.GetNUMARegions(&numa_regions);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t count = 0;
      for (auto it = read_table.begin(numa_regions[id]); it != read_table.end(numa_regions[id]); it++) {
        count++;
      }
    };

    std::promise<void> promises[numa_regions.size()];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t j = 0; j < numa_regions.size(); j++) {
        thread_pool.SubmitTask(&promises[j], [j, &workload] { workload(j); });
      }

      for (auto &promise : promises) {  // NOLINT
        promise.get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, NUMAMultiThreadedNUMAAwareIteration)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  FillAcrossNUMARegions(&read_table);

  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids = GetOneCPUPerRegion();
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  std::vector<common::numa_region_t> numa_regions;
  read_table.GetNUMARegions(&numa_regions);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      uint32_t count = 0;
      for (auto it = read_table.begin(numa_regions[id]); it != read_table.end(numa_regions[id]); it++) {
        count++;
      }
    };

    std::promise<void> promises[numa_regions.size()];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t j = 0; j < numa_regions.size(); j++) {
        thread_pool.SubmitTask(
            &promises[j], [j, &workload] { workload(j); }, numa_regions[j]);
      }

      // NOLINTNEXTLINE
      for (auto &promise : promises) {  // NOLINT
        promise.get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentIterationNoContextSwitching)(benchmark::State &state) {
  uint32_t num_tables = 3;
  uint32_t num_iterators_per_table = 20;
  uint32_t tuples_per_table = num_reads_ / num_tables;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::DataTable *> read_tables(num_tables);
    for (uint32_t i = 0; i < num_tables; i++) {
      read_tables[i] = new storage::DataTable(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                              storage::layout_version_t(0));
    }

    for (auto read_table : read_tables) {
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                          common::ManagedPointer(&buffer_pool_), DISABLED);
      for (uint32_t i = 0; i < tuples_per_table; i++) {
        read_table->Insert(common::ManagedPointer(&txn), *redo_);
      }
    }

    std::vector<std::function<void()>> lambdas;
    for (uint32_t i = 0; i < num_tables * num_iterators_per_table; i++) {
      lambdas.emplace_back([&, i] {
        uint64_t count = 0;
        auto table = read_tables[i % num_tables];
        for (auto it UNUSED_ATTRIBUTE = table->begin(); it != table->end(); it++) count++;
      });
    }

    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids;
    for (uint32_t i = 0; i < std::thread::hardware_concurrency(); i++) cpu_ids.emplace_back(i);
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry),
                                            &cpu_ids);

    std::promise<void> promises[lambdas.size()];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i]);
      }

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    for (auto read_table : read_tables) {
      delete read_table;
    }
  }
  state.SetItemsProcessed(state.iterations() * tuples_per_table * num_iterators_per_table * num_tables);
}

// Read the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentIterationWithContextSwitching)(benchmark::State &state) {
  uint32_t num_tables = std::thread::hardware_concurrency();
  uint32_t num_iterators_per_table = 20;
  uint32_t tuples_per_table = num_reads_ / num_tables;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::DataTable *> read_tables(num_tables);
    for (uint32_t i = 0; i < num_tables; i++) {
      read_tables[i] = new storage::DataTable(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                              storage::layout_version_t(0));
    }

    for (auto read_table : read_tables) {
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                          common::ManagedPointer(&buffer_pool_), DISABLED);
      for (uint32_t i = 0; i < tuples_per_table; i++) {
        read_table->Insert(common::ManagedPointer(&txn), *redo_);
      }
    }

    std::vector<std::function<void(common::PoolContext *)>> lambdas;
    for (uint32_t i = 0; i < num_tables * num_iterators_per_table; i++) {
      lambdas.emplace_back([&, i](common::PoolContext *ctx) {
        uint64_t count = 0;
        auto table = read_tables[i % num_tables];
        for (auto it UNUSED_ATTRIBUTE = table->begin(ctx); it != table->end(ctx); it++) count++;
      });
    }

    common::DedicatedThreadRegistry registry(DISABLED);
    std::vector<int> cpu_ids;
    for (uint32_t i = 0; i < std::thread::hardware_concurrency(); i++) cpu_ids.emplace_back(i);
    common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry),
                                            &cpu_ids);

    std::promise<void> promises[lambdas.size()];
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // Single threaded iteration
      for (uint32_t i = 0; i < lambdas.size(); i++) {
        thread_pool.SubmitTask(&promises[i], lambdas[i]);
      }

      for (uint32_t i = 0; i < lambdas.size(); i++) {
        promises[i].get_future().get();
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    for (auto read_table : read_tables) {
      delete read_table;
    }
  }
  state.SetItemsProcessed(state.iterations() * tuples_per_table * num_iterators_per_table * num_tables);
}

// ----------------------------------------------------------------------------
// Benchmark Registration
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(DataTableBenchmark, Insert)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, SelectRandom)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, SelectSequential)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, Scan)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, SingleThreadedIteration)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, NUMASingleThreadedIteration)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, NUMAMultiThreadedIteration)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, NUMAMultiThreadedNUMAAwareIteration)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentIterationNoContextSwitching)
->Unit(benchmark::kMillisecond)
->UseRealTime()
->UseManualTime();
BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentIterationWithContextSwitching)
->Unit(benchmark::kMillisecond)
->UseRealTime()
->UseManualTime();
// clang-format on

}  // namespace terrier
