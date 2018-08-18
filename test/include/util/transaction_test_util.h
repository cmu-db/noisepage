#pragma once
#include <unordered_map>
#include <map>
#include <utility>
#include <algorithm>
#include <vector>
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "common/container/concurrent_vector.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "gtest/gtest.h"

namespace terrier {
class LargeTransactionTestObject;
class RandomWorkloadTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
using VersionedSnapshots = std::map<timestamp_t, TableSnapshot>;
// {committed, aborted}
using SimulationResult = std::pair<std::vector<RandomWorkloadTransaction *>, std::vector<RandomWorkloadTransaction *>>;

/**
 * A RandomWorkloadTransaction class provides a simple interface to simulate a transaction running in the system.
 *
 * The transaction can be initialized to store enough information to allow for correctness checking, or take care
 * of transaction recycling when GC is not turned on. Disable correctness record-keeping for test cases where you
 * don't care about correctness.
 */
// TODO(Tianyu): We do not support inserts and deletes interleaved in this randomized test yet. Given our storage model,
// those operations are not really different from updates except the execution layer will interpret tuples differently,
// and GC needs to recycle slots. Maybe we can write those later, but I suspect the gain will be minimal compared to
// the extra effort.
class RandomWorkloadTransaction {
 public:
  RandomWorkloadTransaction(LargeTransactionTestObject *test_object);

  ~RandomWorkloadTransaction();

  template<class Random>
  void RandomUpdate(Random *generator);

  template<class Random>
  void RandomSelect(Random *generator);

  void Finish();

 private:
  friend class LargeTransactionTestObject;
  LargeTransactionTestObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  timestamp_t start_time_, commit_time_;
  std::vector<TupleEntry> selects_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
  byte *buffer_;
};

class LargeTransactionTestObject {
 public:
  LargeTransactionTestObject(uint16_t max_columns,
                             uint32_t initial_table_size,
                             uint32_t txn_length,
                             std::vector<double> update_select_ratio,
                             storage::BlockStore *block_store,
                             common::ObjectPool<transaction::UndoBufferSegment> *buffer_pool,
                             std::default_random_engine *generator,
                             bool gc_on,
                             bool bookkeeping);

  ~LargeTransactionTestObject();

  SimulationResult SimulateOltp(uint32_t num_transactions,
                                uint32_t num_concurrent_txns);

  void CheckReadsCorrect(std::vector<RandomWorkloadTransaction *> *commits);

 private:
  void SimulateOneTransaction(RandomWorkloadTransaction *txn, uint32_t txn_id);

  template<class Random>
  void PopulateInitialTable(uint32_t num_tuples, Random *generator);

  storage::ProjectedRow *CopyTuple(storage::ProjectedRow *other);

  void UpdateSnapshot(RandomWorkloadTransaction *txn,
                      TableSnapshot *curr, const TableSnapshot &before);

  // This returned value will contain memory that has to be freed manually
  VersionedSnapshots ReconstructVersionedTable(std::vector<RandomWorkloadTransaction *> *txns);

  void CheckTransactionReadCorrect(RandomWorkloadTransaction *txn,
                                   const VersionedSnapshots &snapshots);

  friend class RandomWorkloadTransaction;
  uint32_t txn_length_;
  std::vector<double> update_select_ratio_;
  std::default_random_engine *generator_;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager txn_manager_;
  transaction::TransactionContext *initial_txn_;
  bool gc_on_, bookkeeping_;
  // tuple content is meaningless if bookkeeping is off.
  std::vector<TupleEntry> initial_table_;

  // so we don't have to calculate these over and over again
  std::vector<uint16_t> all_cols_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t row_size_ = storage::ProjectedRow::Size(layout_, all_cols_);
};
}  // nanespace terrier
