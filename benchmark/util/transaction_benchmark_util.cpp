#include "util/transaction_benchmark_util.h"
#include <algorithm>
#include <utility>
#include <vector>
#include "common/allocator.h"
#include "transaction/transaction_util.h"

namespace terrier {
RandomWorkloadTransaction::RandomWorkloadTransaction(LargeTransactionBenchmarkObject *test_object)
    : test_object_(test_object),
      txn_(test_object->txn_manager_.BeginTransaction()),
      aborted_(false),
      start_time_(txn_->StartTime()),
      commit_time_(UINT64_MAX),
      buffer_(test_object->bookkeeping_
                  ? nullptr
                  : common::AllocationUtil::AllocateAligned(test_object->row_initializer_.ProjectedRowSize())) {}

RandomWorkloadTransaction::~RandomWorkloadTransaction() {
  if (!test_object_->gc_on_) delete txn_;
  if (!test_object_->bookkeeping_) delete[] buffer_;
  for (auto &entry : updates_) delete[] reinterpret_cast<byte *>(entry.second);
  for (auto &entry : selects_) delete[] reinterpret_cast<byte *>(entry.second);
}

template <class Random>
void RandomWorkloadTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot updated =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  std::vector<col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(test_object_->layout_, generator);
  storage::ProjectedRowInitializer initializer(test_object_->layout_, update_col_ids);
  auto *update_buffer =
      test_object_->bookkeeping_ ? common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize()) : buffer_;
  storage::ProjectedRow *update = initializer.InitializeRow(update_buffer);

  StorageTestUtil::PopulateRandomRow(update, test_object_->layout_, 0.0, generator);
  if (test_object_->bookkeeping_) {
    auto it = updates_.find(updated);
    // don't double update if checking for correctness, as it is complicated to keep track of on snapshots,
    // and not very helpful in finding bugs anyways
    if (it != updates_.end()) {
      delete[] update_buffer;
      return;
    }
    updates_[updated] = update;
  }
  // TODO(Tianyu): Hardly efficient, but will do for testing.
  if (test_object_->wal_on_) {
    auto *record = txn_->StageWrite(nullptr, updated, initializer);
    TERRIER_MEMCPY(record->Delta(), update, update->Size());
  }
  auto result = test_object_->table_.Update(txn_, updated, *update);
  aborted_ = !result;
}

template <class Random>
void RandomWorkloadTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  storage::TupleSlot selected =
      RandomTestUtil::UniformRandomElement(test_object_->last_checked_version_, generator)->first;
  auto *select_buffer = test_object_->bookkeeping_
                            ? common::AllocationUtil::AllocateAligned(test_object_->row_initializer_.ProjectedRowSize())
                            : buffer_;
  storage::ProjectedRow *select = test_object_->row_initializer_.InitializeRow(select_buffer);
  test_object_->table_.Select(txn_, selected, select);
  if (test_object_->bookkeeping_) {
    auto updated = updates_.find(selected);
    // Only track reads whose value depend on the snapshot
    if (updated == updates_.end())
      selects_.emplace_back(selected, select);
    else
      delete[] select_buffer;
  }
}

void RandomWorkloadTransaction::Finish() {
  if (aborted_)
    test_object_->txn_manager_.Abort(txn_);
  else
    commit_time_ = test_object_->txn_manager_.Commit(txn_, [] {});
}

LargeTransactionBenchmarkObject::LargeTransactionBenchmarkObject(const std::vector<uint8_t> &attr_sizes,
                                                                 uint32_t initial_table_size, uint32_t txn_length,
                                                                 std::vector<double> update_select_ratio,
                                                                 storage::BlockStore *block_store,
                                                                 storage::RecordBufferSegmentPool *buffer_pool,
                                                                 std::default_random_engine *generator, bool gc_on,
                                                                 storage::LogManager *log_manager)
    : txn_length_(txn_length),
      update_select_ratio_(std::move(update_select_ratio)),
      generator_(generator),
      layout_({attr_sizes}),
      table_(block_store, layout_, layout_version_t(0)),
      txn_manager_(buffer_pool, gc_on, log_manager),
      gc_on_(gc_on),
      wal_on_(log_manager != LOGGING_DISABLED),
      bookkeeping_(false), abort_count_(0) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTable(initial_table_size, generator_);
}

LargeTransactionBenchmarkObject::~LargeTransactionBenchmarkObject() {
  if (!gc_on_) delete initial_txn_;
  if (bookkeeping_) {
    for (auto &tuple : last_checked_version_) delete[] reinterpret_cast<byte *>(tuple.second);
  }
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeTransactionBenchmarkObject::SimulateOltp(uint32_t num_transactions,
                                                               uint32_t num_concurrent_txns) {
  TestThreadPool thread_pool;
  std::vector<RandomWorkloadTransaction *> txns;
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;
  if (gc_on_ && !bookkeeping_) {
    // Then there is no need to keep track of RandomWorkloadTransaction objects
    workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        RandomWorkloadTransaction txn(this);
        SimulateOneTransaction(&txn, txn_id);
      }
    };
  } else {
    txns.resize(num_transactions);
    // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
    // test objects
    workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        txns[txn_id] = new RandomWorkloadTransaction(this);
        SimulateOneTransaction(txns[txn_id], txn_id);
        //
        if (gc_on_ && !bookkeeping_) delete txns[txn_id];
      }
    };
  }

  thread_pool.RunThreadsUntilFinish(num_concurrent_txns, workload);

    // We only need to deallocate, and return, if gc is on, this loop is a no-op
    for (RandomWorkloadTransaction *txn : txns) {
      abort_count_++;
      delete txn;
    }
    // This result is meaningless if bookkeeping is not turned on.
    return abort_count_;
}

void LargeTransactionBenchmarkObject::SimulateOneTransaction(terrier::RandomWorkloadTransaction *txn, uint32_t txn_id) {
  std::default_random_engine thread_generator(txn_id);

  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  RandomTestUtil::InvokeWorkloadWithDistribution({update, select}, update_select_ratio_, &thread_generator,
                                                 txn_length_);
  txn->Finish();
}

template <class Random>
void LargeTransactionBenchmarkObject::PopulateInitialTable(uint32_t num_tuples, Random *generator) {
  initial_txn_ = txn_manager_.BeginTransaction();
  byte *redo_buffer = nullptr;
  if (!bookkeeping_) {
    // If no bookkeeping is required we can reuse the same buffer over and over again.
    redo_buffer = common::AllocationUtil::AllocateAligned(row_initializer_.ProjectedRowSize());
    row_initializer_.InitializeRow(redo_buffer);
  }
  for (uint32_t i = 0; i < num_tuples; i++) {
    // Otherwise we will need to get a new redo buffer each insert so we log the values inserted.
    if (bookkeeping_) redo_buffer = common::AllocationUtil::AllocateAligned(row_initializer_.ProjectedRowSize());
    // reinitialize every time only if bookkeeping;
    storage::ProjectedRow *redo = bookkeeping_ ? row_initializer_.InitializeRow(redo_buffer)
                                               : reinterpret_cast<storage::ProjectedRow *>(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_.Insert(initial_txn_, *redo);
    // TODO(Tianyu): Hardly efficient, but will do for testing.
    if (wal_on_) {
      auto *record = initial_txn_->StageWrite(nullptr, inserted, row_initializer_);
      TERRIER_MEMCPY(record->Delta(), redo, redo->Size());
    }
    last_checked_version_.emplace_back(inserted, bookkeeping_ ? redo : nullptr);
  }
  txn_manager_.Commit(initial_txn_, [] {});
  // cleanup if not keeping track of all the inserts.
  if (!bookkeeping_) delete[] redo_buffer;
}
}  // namespace terrier
