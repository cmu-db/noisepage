#include "util/data_table_benchmark_util.h"
#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>
#include "common/allocator.h"
#include "transaction/transaction_util.h"
#include "util/catalog_test_util.h"

namespace terrier {
RandomDataTableTransaction::RandomDataTableTransaction(LargeDataTableBenchmarkObject *test_object)
    : test_object_(test_object),
      txn_(test_object->txn_manager_.BeginTransaction()),
      aborted_(false),
      start_time_(txn_->StartTime()),
      commit_time_(UINT64_MAX),
      buffer_(common::AllocationUtil::AllocateAligned(test_object->row_initializer_.ProjectedRowSize())) {}

RandomDataTableTransaction::~RandomDataTableTransaction() {
  if (!test_object_->gc_on_) delete txn_;
  delete[] buffer_;
}

template <class Random>
void RandomDataTableTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  const storage::TupleSlot updated = *(RandomTestUtil::UniformRandomElement(test_object_->inserted_tuples_, generator));
  std::vector<storage::col_id_t> update_col_ids =
      StorageTestUtil::ProjectionListRandomColumns(test_object_->layout_, generator);
  storage::ProjectedRowInitializer initializer =
      storage::ProjectedRowInitializer::Create(test_object_->layout_, update_col_ids);

  auto *const record = txn_->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, initializer);
  record->SetTupleSlot(updated);

  StorageTestUtil::PopulateRandomRow(record->Delta(), test_object_->layout_, 0.0, generator);
  auto result = test_object_->table_.Update(txn_, updated, *(record->Delta()));
  aborted_ = !result;
}

template <class Random>
void RandomDataTableTransaction::RandomInsert(Random *generator) {
  if (aborted_) return;
  auto *const redo =
      txn_->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, test_object_->row_initializer_);
  StorageTestUtil::PopulateRandomRow(redo->Delta(), test_object_->layout_, 0.0, generator);
  const storage::TupleSlot inserted = test_object_->table_.Insert(txn_, *(redo->Delta()));
  redo->SetTupleSlot(inserted);
}

template <class Random>
void RandomDataTableTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  const storage::TupleSlot selected =
      *(RandomTestUtil::UniformRandomElement(test_object_->inserted_tuples_, generator));
  auto *select_buffer = buffer_;
  storage::ProjectedRow *select = test_object_->row_initializer_.InitializeRow(select_buffer);
  test_object_->table_.Select(txn_, selected, select);
}

void RandomDataTableTransaction::Finish() {
  if (aborted_)
    test_object_->txn_manager_.Abort(txn_);
  else
    commit_time_ = test_object_->txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
}

// TODO(Tianyu): Convert to DI
LargeDataTableBenchmarkObject::LargeDataTableBenchmarkObject(const std::vector<uint8_t> &attr_sizes,
                                                             uint32_t initial_table_size, uint32_t txn_length,
                                                             std::vector<double> operation_ratio,
                                                             storage::BlockStore *block_store,
                                                             storage::RecordBufferSegmentPool *buffer_pool,
                                                             std::default_random_engine *generator, bool gc_on,
                                                             storage::LogManager *log_manager)
    : txn_length_(txn_length),
      operation_ratio_(std::move(operation_ratio)),
      generator_(generator),
      layout_({attr_sizes}),
      table_(block_store, layout_, storage::layout_version_t(0)),
      txn_manager_(&timestamp_manager_, DISABLED, buffer_pool, gc_on, log_manager),
      gc_on_(gc_on),
      abort_count_(0) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTable(initial_table_size, generator_);
}

LargeDataTableBenchmarkObject::~LargeDataTableBenchmarkObject() {
  if (!gc_on_) delete initial_txn_;
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeDataTableBenchmarkObject::SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  std::vector<RandomDataTableTransaction *> txns;
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;
  if (gc_on_) {
    // Then there is no need to keep track of RandomWorkloadTransaction objects
    workload = [&](uint32_t /*unused*/) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        RandomDataTableTransaction txn(this);
        SimulateOneTransaction(&txn, txn_id);
      }
    };
  } else {
    txns.resize(num_transactions);
    // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
    // test objects
    workload = [&](uint32_t /*unused*/) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        txns[txn_id] = new RandomDataTableTransaction(this);
        SimulateOneTransaction(txns[txn_id], txn_id);
      }
    };
  }

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns, workload);

  // We only need to deallocate, and return, if gc is on, this loop is a no-op
  for (RandomDataTableTransaction *txn : txns) {
    if (txn->aborted_) abort_count_++;
    delete txn;
  }
  // This result is meaningless if bookkeeping is not turned on.
  return abort_count_;
}

void LargeDataTableBenchmarkObject::SimulateOneTransaction(terrier::RandomDataTableTransaction *txn, uint32_t txn_id) {
  std::default_random_engine thread_generator(txn_id);

  auto insert = [&] { txn->RandomInsert(&thread_generator); };
  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  RandomTestUtil::InvokeWorkloadWithDistribution({insert, update, select}, operation_ratio_, &thread_generator,
                                                 txn_length_);
  txn->Finish();
}

template <class Random>
void LargeDataTableBenchmarkObject::PopulateInitialTable(uint32_t num_tuples, Random *generator) {
  initial_txn_ = txn_manager_.BeginTransaction();

  for (uint32_t i = 0; i < num_tuples; i++) {
    auto *const redo =
        initial_txn_->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, row_initializer_);
    StorageTestUtil::PopulateRandomRow(redo->Delta(), layout_, 0.0, generator);
    const storage::TupleSlot inserted = table_.Insert(initial_txn_, *(redo->Delta()));
    redo->SetTupleSlot(inserted);
    inserted_tuples_.emplace_back(inserted);
  }
  txn_manager_.Commit(initial_txn_, TestCallbacks::EmptyCallback, nullptr);
}
}  // namespace terrier
