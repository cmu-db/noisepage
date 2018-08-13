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

    updates_.emplace_back(updated, update);

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
    if (aborted_)
      txn_manager_->Abort(txn_);
    else {
      txn_manager_->Commit(txn_);
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
  std::vector<entry> updates_, selects_, inserts_;
  bool aborted_ = false;

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
};

class LargeTransactionTestObject {
 public:
  LargeTransactionTestObject(uint16_t max_columns,
                             storage::BlockStore *block_store,
                             common::ObjectPool<transaction::UndoBufferSegment> *buffer_pool,
                             std::default_random_engine *generator)
      : generator_(generator),
        layout_(StorageTestUtil::RandomLayout(max_columns, generator_)),
        table_(block_store, layout_),
        txn_manager_(buffer_pool) {}

  void SimulateOneTransaction(RandomWorkloadTransaction *txn, uint32_t txn_id) {
    std::default_random_engine thread_generator(txn_id);

    auto insert = [&] { txn->RandomInsert(&thread_generator); };
    auto update = [&] { txn->RandomUpdate(&thread_generator); };
    auto select = [&] { txn->RandomSelect(&thread_generator); };
    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({insert, update, select},
                                                          {0.1, 0.2, 0.7},
                                                          &thread_generator,
                                                          100);
    txn->Finish();
  }

  std::vector<RandomWorkloadTransaction *> SimulateOltp(uint32_t num_transactions,
                                                        uint32_t num_concurrent_txns) {
    std::vector<RandomWorkloadTransaction *> result(num_transactions);
    volatile std::atomic<uint32_t> txns_run = 0;
    auto workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        result[txn_id] = new RandomWorkloadTransaction(layout_, &table_, &txn_manager_, &all_slots);
        SimulateOneTransaction(result[txn_id], txn_id);
      }
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_concurrent_txns, workload);
    return result;
  }

//  using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow>;
//  std::unordered_map<timestamp_t,
//                     TableSnapshot> ReconstructVersionedTable(std::vector<RandomWorkloadTransaction> *txns) {
//
//  }

  void CheckTransactionConsistent(std::vector<RandomWorkloadTransaction> *txns) {
  }

 private:
  std::default_random_engine *generator_ UNUSED_ATTRIBUTE;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager txn_manager_;
  ConcurrentVectorWithSize<storage::TupleSlot> all_slots;
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
  const uint32_t num_txns = 1000;
  const uint32_t num_concurrent_txns = 8;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, &block_store_, &buffer_pool_, &generator_);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    for (auto w : result) delete w;
  }
}

//TEST(WTF, ConcurrentVector) {
//  ConcurrentVectorWithSize<int> tested;
//  auto workload = [&](uint32_t id) {
//    std::default_random_engine r(id);
//    auto insert = [&] {
//      tested.PushBack(42);
//    };
//    auto read = [&] {
//      EXPECT_EQ(42, tested.RandomElement(&r));
//    };
//    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({insert, read}, {0.8, 0.2}, &r, 1000);
//  };
//  MultiThreadedTestUtil::RunThreadsUntilFinish(8, workload, 1000);
//}
}  // namespace terrier
