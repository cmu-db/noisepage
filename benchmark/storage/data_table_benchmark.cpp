#include <unordered_map>
#include <utility>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "util/multi_threaded_test_util.h"
#include "util/storage_benchmark_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {

// Test raw DataTable insert time. Generate a fixed layout allocate undo and redo buffers, and then reuse them to
// repeatedly insert the same garbage tuple over and over into the DataTable to test throughput.
// NOLINTNEXTLINE
static void BM_SimpleInsert(benchmark::State &state) {
  std::default_random_engine generator;
  const uint32_t num_inserts = 100000;

  storage::BlockStore block_store{1000};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool{num_inserts};
  transaction::TransactionManager txn_manager(&buffer_pool);

  // Tuple layout
  uint16_t num_columns = 2;
  uint8_t column_size = 8;
  storage::BlockLayout layout(num_columns, {column_size, column_size});

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout, all_col_ids_);
  uint32_t undo_size_ = storage::DeltaRecord::Size(layout, all_col_ids_);

  // generate a random redo ProjectedRow to Insert
  byte *redo_buffer = new byte[redo_size_];
  storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout);
  StorageTestUtil::PopulateRandomRow(redo, layout, 0, &generator);

  // Populate the table with tuples
  while (state.KeepRunning()) {
    storage::DataTable table(&block_store, layout);
    for (uint32_t i = 0; i < num_inserts; ++i) {
      // TODO(Tianyu): Figure out if we need this
      auto *txn = txn_manager.BeginTransaction();
      table.Insert(txn, *redo);
      txn_manager.Commit(txn);
    }
  }

  delete[] redo_buffer;

  state.SetBytesProcessed(state.iterations() * num_inserts * (redo_size_ + undo_size_));
  state.SetItemsProcessed(state.iterations() * num_inserts);
}


// Test raw DataTable insert time concurrently. Generate a fixed layout allocate undo and redo buffers, and then reuse
// them to repeatedly insert the same garbage tuple over and over into the DataTable to test throughput. Expect high
// contention on this benchmark right now due to inserting into a single block in the DataTable.
// NOLINTNEXTLINE
static void BM_ConcurrentInsert(benchmark::State &state) {
  std::default_random_engine generator;
  const uint32_t num_inserts = 10000;

  storage::BlockStore block_store{1000};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool{num_inserts};

  // Tuple layout
  uint16_t num_columns = 2;
  uint8_t column_size = 8;
  storage::BlockLayout layout(num_columns, {column_size, column_size});

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout, all_col_ids_);
  uint32_t undo_size_ = storage::DeltaRecord::Size(layout, all_col_ids_);

  const uint32_t num_threads = 8;

  // generate a random redo ProjectedRow to Insert
  byte *redo_buffer = new byte[redo_size_];
  storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout);
  StorageTestUtil::PopulateRandomRow(redo, layout, 0, &generator);

  while (state.KeepRunning()) {
    storage::DataTable table(&block_store, layout);
    auto workload = [&](uint32_t id) {
      transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool);
      for (uint32_t i = 0; i < num_inserts / num_threads; i++)
        table.Insert(&txn, *redo);
    };
    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads, workload);
  }

  delete[] redo_buffer;

  state.SetBytesProcessed(state.iterations() * num_inserts * (redo_size_ + undo_size_));
  state.SetItemsProcessed(state.iterations() * num_inserts);
}

BENCHMARK(BM_SimpleInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_ConcurrentInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace terrier
