#include <unordered_map>
#include <emmintrin.h>
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "common/container/concurrent_vector.h"
#include "util/storage_test_util.h"
#include "gtest/gtest.h"

namespace terrier {
template<class T>
class ConcurrentVectorWithSize {
 public:
  void PushBack(T val) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    vector_.push_back(val);
  }

  template<class Random>
  T RandomElement(Random *generator) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    T result = vector_[std::uniform_int_distribution(0, static_cast<int>(vector_.size() - 1))(*generator)];
    return result;
  }

  bool Empty() {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    bool result = vector_.empty();
    return result;
  }

 private:
  common::SpinLatch latch_;
  std::vector<T> vector_;
};

class RandomWorkloadTransaction {
 public:
  RandomWorkloadTransaction(const storage::BlockLayout &layout,
                            storage::DataTable *table,
                            transaction::TransactionManager *txn_manager,
                            ConcurrentVectorWithSize<storage::TupleSlot> *all_slots)
      : layout_(layout),
        table_(table),
        txn_manager_(txn_manager),
        txn_(txn_manager->BeginTransaction()),
        all_slots_(all_slots) {}

  ~RandomWorkloadTransaction() {
    delete txn_;
    for (auto &entry : updates_)
      delete[] reinterpret_cast<byte *>(entry.second);
    for (auto &entry : selects_)
      delete[] reinterpret_cast<byte *>(entry.second);
  }

  template<class Random>
  void RandomInsert(Random *generator) {
    if (aborted_) return;
    byte *redo_buffer = new byte[redo_size_];
    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_->Insert(txn_, *redo);
    inserts_.emplace_back(inserted, redo);
  }

  template<class Random>
  void RandomUpdate(Random *generator) {
    if (all_slots_->Empty() || aborted_) return;
    storage::TupleSlot updated = all_slots_->RandomElement(generator);

    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    byte *update_buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, 0.0, generator);

    // don't want to keep track of updates that are overshadowed by a later operation.
    auto it = updates_.find(updated);
    if (it != updates_.end()) return;
    updates_[updated] = update;
    auto result = table_->Update(txn_, updated, *update);
    aborted_ = !result;
  }

  template<class Random>
  void RandomSelect(Random *generator) {
    if (all_slots_->Empty() || aborted_) return;
    storage::TupleSlot selected = all_slots_->RandomElement(generator);
    byte *select_buffer = new byte[redo_size_];
    storage::ProjectedRow *select = storage::ProjectedRow::InitializeProjectedRow(select_buffer, all_col_ids_, layout_);
    table_->Select(txn_, selected, select);
    selects_.emplace_back(selected, select);
  }

  void Finish() {
    if (aborted_) {
      txn_manager_->Abort(txn_);
      commit_time_ = timestamp_t(-1);
    } else {
      commit_time_ = txn_manager_->Commit(txn_);
      for (auto &entry : inserts_)
        all_slots_->PushBack(entry.first);
    }
  }

 private:
  friend class LargeTransactionTestObject;
  const storage::BlockLayout &layout_;
  storage::DataTable *table_;

  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *txn_;
  // This will not work if GC is turned on
  ConcurrentVectorWithSize<storage::TupleSlot> *all_slots_;
  using entry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
  std::vector<entry> selects_, inserts_;

  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
  bool aborted_ = false;
  timestamp_t commit_time_{0};

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
};

class LargeTransactionTestObject {
 public:
  using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
  using VersionedSnapshots = std::map<timestamp_t, TableSnapshot>;

  LargeTransactionTestObject(uint16_t max_columns,
                             storage::BlockStore *block_store,
                             common::ObjectPool<transaction::UndoBufferSegment> *buffer_pool,
                             std::default_random_engine *generator)
      : generator_(generator),
        layout_(2, {8, 8}),
//        layout_(StorageTestUtil::RandomLayout(max_columns, generator_)),
        table_(block_store, layout_),
        txn_manager_(buffer_pool),
        initial_txn_(layout_, &table_, &txn_manager_, &all_slots_) {
    for (uint32_t i = 0; i < 10; i++)
      initial_txn_.RandomInsert(generator_);
    initial_txn_.Finish();
  }

  void SimulateOneTransaction(RandomWorkloadTransaction *txn, uint32_t txn_id) {
    std::default_random_engine thread_generator(txn_id);

//    auto insert = [&] { txn->RandomInsert(&thread_generator); };
    auto update = [&] { txn->RandomUpdate(&thread_generator); };
    auto select = [&] { txn->RandomSelect(&thread_generator); };
    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({update, select},
                                                          {0.3, 0.7},
                                                          &thread_generator,
                                                          20);
    txn->Finish();
  }

  std::pair<std::vector<RandomWorkloadTransaction *>, std::vector<RandomWorkloadTransaction *>>
                                           SimulateOltp(uint32_t num_transactions,
                                                        uint32_t num_concurrent_txns) {
    std::vector<RandomWorkloadTransaction *> result(num_transactions);
    volatile std::atomic<uint32_t> txns_run = 0;
    auto workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        result[txn_id] = new RandomWorkloadTransaction(layout_, &table_, &txn_manager_, &all_slots_);
        SimulateOneTransaction(result[txn_id], txn_id);
      }
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_concurrent_txns, workload);
    // filter out aborted transactions
    std::vector<RandomWorkloadTransaction *> committed;
    std::vector<RandomWorkloadTransaction *> aborts;
    for (RandomWorkloadTransaction *txn : result) {
      if (!txn->aborted_)
        committed.push_back(txn);
      else
        aborts.push_back(txn);  // will never look at aborted transactions
    }

    // Sort according to commit timestamp
    std::sort(committed.begin(), committed.end(),
              [](RandomWorkloadTransaction *a, RandomWorkloadTransaction *b) {
                return transaction::TransactionUtil::NewerThan(b->commit_time_, a->commit_time_);
              });
    return {committed, aborts};
  }

  void CheckReadsCorrect(std::vector<RandomWorkloadTransaction *> *commits, std::vector<RandomWorkloadTransaction *> *aborts) {
    VersionedSnapshots snapshots = ReconstructVersionedTable(commits);
    // Only need to check that reads make sense?
    for (RandomWorkloadTransaction *txn : *commits)
      CheckTransactionReadCorrect(txn, snapshots, commits, aborts);

    // delete versions that are outdated
    for (auto &snapshot : snapshots)
      for (auto &entry : snapshot.second)
        delete[] reinterpret_cast<byte *>(entry.second);
  }

 private:
  storage::ProjectedRow *CopyTuple(storage::ProjectedRow *other) {
    byte *copy = new byte[other->Size()];
    PELOTON_MEMCPY(copy, other, other->Size());
    return reinterpret_cast<storage::ProjectedRow *>(copy);
  }

  void UpdateSnapshot(RandomWorkloadTransaction *txn,
                      TableSnapshot *curr, const TableSnapshot &before) {
    for (auto &entry : before)
      curr->emplace(entry.first, CopyTuple(entry.second));
//    for (auto &insert : txn->inserts_)
//      curr->emplace(insert.first, CopyTuple(insert.second));
    for (auto &update : txn->updates_) {
      storage::ProjectedRow *new_version = CopyTuple((*curr)[update.first]);
      storage::StorageUtil::ApplyDelta(layout_, *update.second, new_version);
      (*curr)[update.first] = new_version;
    }
  }

  // This returned value will contain memory that has to be freed manually
  VersionedSnapshots ReconstructVersionedTable(std::vector<RandomWorkloadTransaction *> *txns) {
    VersionedSnapshots result;
    // empty starting version
    TableSnapshot *prev = &(result.emplace(timestamp_t(0), TableSnapshot()).first->second);
    for (auto &entry : initial_txn_.inserts_)
      (*prev)[entry.first] = CopyTuple(entry.second);
    for (RandomWorkloadTransaction *txn : *txns) {
      auto ret = result.emplace(txn->commit_time_, TableSnapshot());
      UpdateSnapshot(txn, &(ret.first->second), *prev);
      prev = &(ret.first->second);
    }
    return result;
  }

  void PrintTransactionLog(RandomWorkloadTransaction *txn, storage::TupleSlot slot) {
    printf("\n\n*******begin txn*********\n");
    printf("transaction starting at: %llu\n", !txn->txn_->StartTime());
    printf("relevant reads to slot:\n");
    for (auto &entry : txn->selects_) {
      if (entry.first == slot) StorageTestUtil::PrintRow(*entry.second, layout_);
    }
    printf("relevant writes to slot:\n");
    for (auto &entry : txn->updates_) {
      if (entry.first == slot) StorageTestUtil::PrintRow(*entry.second, layout_);
    }
    printf("transaction committed at: %llu\n", !txn->commit_time_);
    printf("*******end txn*********\n");
  }

  void CheckTransactionReadCorrect(RandomWorkloadTransaction *txn,
      const VersionedSnapshots &snapshots, std::vector<RandomWorkloadTransaction *> *commits, std::vector<RandomWorkloadTransaction *> *aborts) {
    timestamp_t start_time = txn->txn_->StartTime();
    if (start_time == timestamp_t(0)) return;  // first transaction cannot perform a read or update
    // this version is the most recent future update
    auto ret = snapshots.upper_bound(start_time);
    // Go to the version visible to this txn
    --ret;
    timestamp_t version_timestamp = ret->first;
    const TableSnapshot &before_snapshot = ret->second;
    EXPECT_TRUE(transaction::TransactionUtil::NewerThan(start_time, version_timestamp));
    for (auto &entry : txn->selects_) {
      // Cannot check the tuples this transaction updated because the read would be for their own updated version.
      if (txn->updates_.find(entry.first) != txn->updates_.end()) continue;
      auto it = before_snapshot.find(entry.first);
//      if (!StorageTestUtil::ProjectionListEqual(layout_, entry.second, it->second)) {
//        printf("\nerror version_timestamp: %llu\n", !start_time);
//        printf("expected:\n");
//        StorageTestUtil::PrintRow(*it->second, layout_);
//        printf("actual:\n");
//        StorageTestUtil::PrintRow(*entry.second, layout_);
//        printf("version history below:\n");
//        for (auto &snap : snapshots) {
//          printf("at timestamp %llu:\n", !snap.first);
//          StorageTestUtil::PrintRow(*(snap.second.find(entry.first)->second), layout_);
//        }
//        for (auto txn : *commits)
//          PrintTransactionLog(txn, entry.first);
//
//        printf("\n\n\n aborted txns: \n\n\n");
//        for (auto txn : *aborts)
//          PrintTransactionLog(txn, entry.first);
//        PELOTON_ASSERT(false, "");
//      }
       EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(layout_, entry.second, it->second));
    }
  }

  std::default_random_engine *generator_ UNUSED_ATTRIBUTE;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager txn_manager_;
  ConcurrentVectorWithSize<storage::TupleSlot> all_slots_;
  RandomWorkloadTransaction initial_txn_;
};

class LargeTransactionTests : public ::testing::Test {
 public:
  storage::BlockStore block_store_{1000};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{10000};
  std::default_random_engine generator_;
};

TEST_F(LargeTransactionTests, MixedReadWrite) {
  const uint32_t num_iterations = 10000;
  const uint16_t max_columns = 20;
  const uint32_t num_txns = 500;
  const uint32_t num_concurrent_txns = 8;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, &block_store_, &buffer_pool_, &generator_);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first, &result.second);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}
}  // namespace terrier
