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

// TODO(Tianyu): Some of these code might qualify for StorageTestUtil, as we may also use them in GC tests.
class RandomWorkloadTransaction {
 public:
  RandomWorkloadTransaction(const storage::BlockLayout &layout,
                            storage::DataTable *table,
                            transaction::TransactionManager *txn_manager,
                            const std::vector<storage::TupleSlot> &all_slots)
      : layout_(layout),
        table_(table),
        txn_manager_(txn_manager),
        txn_(txn_manager->BeginTransaction()),
        all_slots_(all_slots) {}

  ~RandomWorkloadTransaction() {
    delete txn_;
    for (auto &entry : inserts_)
      delete[] reinterpret_cast<byte *>(entry.second);
    for (auto &entry : updates_)
      delete[] reinterpret_cast<byte *>(entry.second);
    for (auto &entry : selects_)
      delete[] reinterpret_cast<byte *>(entry.second);
  }

  template<class Random>
  void RandomInsert(Random *generator) {
    if (aborted_) return;
    auto *redo_buffer = new byte[redo_size_];
    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_->Insert(txn_, *redo);
    inserts_.emplace_back(inserted, redo);
  }

  template<class Random>
  void RandomUpdate(Random *generator) {
    if (aborted_) return;
    storage::TupleSlot updated = *MultiThreadedTestUtil::UniformRandomElement(all_slots_, generator);

    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    auto *update_buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, 0.0, generator);

    auto it = updates_.find(updated);
    // don't want to double update, as it is complicated to keep track of on snapshots, and not very helpful
    // in finding bugs anyways
    if (it != updates_.end()) {
      delete[] update_buffer;
      return;
    }
    updates_[updated] = update;
    auto result = table_->Update(txn_, updated, *update);
    aborted_ = !result;
  }

  template<class Random>
  void RandomSelect(Random *generator) {
    if (aborted_) return;
    storage::TupleSlot selected = *MultiThreadedTestUtil::UniformRandomElement(all_slots_, generator);
    auto *select_buffer = new byte[redo_size_];
    storage::ProjectedRow *select = storage::ProjectedRow::InitializeProjectedRow(select_buffer, all_col_ids_, layout_);
    table_->Select(txn_, selected, select);
    auto updated = updates_.find(selected);
    // Only track reads whose value depend on the snapshot
    if (updated == updates_.end())
      selects_.emplace_back(selected, select);
    else
      delete[] select_buffer;
  }

  void Finish() {
    if (aborted_)
      txn_manager_->Abort(txn_);
    else
      commit_time_ = txn_manager_->Commit(txn_);
  }

 private:
  friend class LargeTransactionTestObject;
  const storage::BlockLayout &layout_;
  storage::DataTable *table_;

  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *txn_;
  // This will not work if GC is turned on
  const std::vector<storage::TupleSlot> &all_slots_;
  using entry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
  std::vector<entry> selects_, inserts_;

  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
  bool aborted_ = false;
  timestamp_t commit_time_{static_cast<uint64_t>(-1)};

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
};

class LargeTransactionTestObject {
 public:
  using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
  using VersionedSnapshots = std::map<timestamp_t, TableSnapshot>;

  LargeTransactionTestObject(uint16_t max_columns,
                             uint32_t initial_table_size,
                             uint32_t txn_length,
                             std::vector<double> update_select_ratio,
                             storage::BlockStore *block_store,
                             common::ObjectPool<transaction::UndoBufferSegment> *buffer_pool,
                             std::default_random_engine *generator)
      : txn_length_(txn_length),
        update_select_ratio_(std::move(update_select_ratio)),
        generator_(generator),
        layout_(StorageTestUtil::RandomLayout(max_columns, generator_)),
        table_(block_store, layout_),
        txn_manager_(buffer_pool),
        initial_txn_(layout_, &table_, &txn_manager_, all_slots_) {
    // Bootstrap the table to have the specified number of tuples
    for (uint32_t i = 0; i < initial_table_size; i++)
      initial_txn_.RandomInsert(generator_);
    initial_txn_.Finish();
    for (auto &entry : initial_txn_.inserts_)
      all_slots_.push_back(entry.first);
  }

  void SimulateOneTransaction(RandomWorkloadTransaction *txn, uint32_t txn_id) {
    std::default_random_engine thread_generator(txn_id);

    auto update = [&] { txn->RandomUpdate(&thread_generator); };
    auto select = [&] { txn->RandomSelect(&thread_generator); };
    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({update, select},
                                                          update_select_ratio_,
                                                          &thread_generator,
                                                          txn_length_);
    txn->Finish();
  }

   std::vector<RandomWorkloadTransaction *> SimulateOltp(uint32_t num_transactions,
                                                        uint32_t num_concurrent_txns) {
    std::vector<RandomWorkloadTransaction *> result(num_transactions);
    volatile std::atomic<uint32_t> txns_run = 0;
    auto workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        result[txn_id] = new RandomWorkloadTransaction(layout_, &table_, &txn_manager_, all_slots_);
        SimulateOneTransaction(result[txn_id], txn_id);
      }
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_concurrent_txns, workload);
    // filter out aborted transactions
    std::vector<RandomWorkloadTransaction *> committed;
    std::vector<RandomWorkloadTransaction *> aborted;
    for (RandomWorkloadTransaction *txn : result) {
      if (!txn->aborted_)
        committed.push_back(txn);
      else
        delete txn;  // will never look at aborted transactions
    }

    // Sort according to commit timestamp
    std::sort(committed.begin(), committed.end(),
              [](RandomWorkloadTransaction *a, RandomWorkloadTransaction *b) {
                return transaction::TransactionUtil::NewerThan(b->commit_time_, a->commit_time_);
              });
    return committed;
  }

  void CheckReadsCorrect(std::vector<RandomWorkloadTransaction *> *commits) {
    VersionedSnapshots snapshots = ReconstructVersionedTable(commits);
    // Only need to check that reads make sense?
    for (RandomWorkloadTransaction *txn : *commits)
      CheckTransactionReadCorrect(txn, snapshots);

    // clean up memory
    for (auto &snapshot : snapshots)
      for (auto &entry : snapshot.second)
        delete[] reinterpret_cast<byte *>(entry.second);
  }

 private:
  storage::ProjectedRow *CopyTuple(storage::ProjectedRow *other) {
    auto *copy = new byte[other->Size()];
    PELOTON_MEMCPY(copy, other, other->Size());
    return reinterpret_cast<storage::ProjectedRow *>(copy);
  }

  void UpdateSnapshot(RandomWorkloadTransaction *txn,
                      TableSnapshot *curr, const TableSnapshot &before) {
    for (auto &entry : before)
      curr->emplace(entry.first, CopyTuple(entry.second));
    for (auto &update : txn->updates_) {
      // TODO(Tianyu): Can be smarter about copies
      storage::ProjectedRow *new_version = (*curr)[update.first];
      storage::StorageUtil::ApplyDelta(layout_, *update.second, new_version);
    }
  }

  // This returned value will contain memory that has to be freed manually
  VersionedSnapshots ReconstructVersionedTable(std::vector<RandomWorkloadTransaction *> *txns) {
    VersionedSnapshots result;
    // empty starting version
    TableSnapshot *prev = &(result.emplace(timestamp_t(0), TableSnapshot()).first->second);
    // populate with initial image of the table
    for (auto &entry : initial_txn_.inserts_)
      (*prev)[entry.first] = CopyTuple(entry.second);

    for (RandomWorkloadTransaction *txn : *txns) {
      auto ret = result.emplace(txn->commit_time_, TableSnapshot());
      UpdateSnapshot(txn, &(ret.first->second), *prev);
      prev = &(ret.first->second);
    }
    return result;
  }

  void CheckTransactionReadCorrect(RandomWorkloadTransaction *txn,
                                   const VersionedSnapshots &snapshots) {
    timestamp_t start_time = txn->txn_->StartTime();
    // this version is the most recent future update
    auto ret = snapshots.upper_bound(start_time);
    // Go to the version visible to this txn
    --ret;
    timestamp_t version_timestamp = ret->first;
    const TableSnapshot &before_snapshot = ret->second;
    EXPECT_TRUE(transaction::TransactionUtil::NewerThan(start_time, version_timestamp));
    for (auto &entry : txn->selects_) {
      auto it = before_snapshot.find(entry.first);
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(layout_, entry.second, it->second));
    }
  }
  uint32_t txn_length_;
  std::vector<double> update_select_ratio_;
  std::default_random_engine *generator_;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager txn_manager_;
  std::vector<storage::TupleSlot> all_slots_;
  RandomWorkloadTransaction initial_txn_;
};

class LargeTransactionTests : public ::terrier::TerrierTest {
 public:
  storage::BlockStore block_store_{1000};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{1000};
  std::default_random_engine generator_;
};

// This test case generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWrite) {
  const uint32_t num_iterations = 50000;
  const uint16_t max_columns = 20;
  const uint32_t initial_table_size = 1000;
  const uint32_t txn_length = 100;
  const uint32_t num_txns = 500;
  const std::vector<double> update_select_ratio = {0.3, 0.7};
  const uint32_t num_concurrent_txns = 4;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns,
                                      initial_table_size,
                                      txn_length,
                                      update_select_ratio,
                                      &block_store_,
                                      &buffer_pool_,
                                      &generator_);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result);
    for (auto w : result) delete w;
  }
}
}  // namespace terrier