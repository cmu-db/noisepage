#include <unordered_map>
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "common/container/concurrent_vector.h"
#include "util/storage_test_util.h"
#include "gtest/gtest.h"

namespace terrier {
class RandomWorkloadTransaction {
 public:
  RandomWorkloadTransaction(const storage::BlockLayout &layout,
                            storage::DataTable *table,
                            transaction::TransactionManager *txn_manager,
                            common::ConcurrentVector<storage::TupleSlot> *all_slots)
      : layout_(layout), table_(table), txn_(txn_manager->BeginTransaction()), all_slots_(all_slots) {}

  ~RandomWorkloadTransaction() {
    for (auto &entry : writes_)
      delete[] reinterpret_cast<byte *>(entry.second);
    for (auto &entry : reads_)
      delete[] reinterpret_cast<byte *>(entry.second);
  }

  template<class Random>
  void RandomInsert(Random *generator) {
    byte *redo_buffer = new byte[redo_size_];
    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, 0.0, generator);
    storage::TupleSlot inserted = table_->Insert(txn_, *redo);
    all_slots_->PushBack(inserted);
    writes_.emplace_back(inserted, redo);
  }

  template<class Random>
  bool RandomUpdate(Random *generator) {
    if (all_slots_->Size() == 0) return true;
    storage::TupleSlot updated = *MultiThreadedTestUtil::UniformRandomElement(all_slots_, generator);

    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    byte *update_buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, 0.0, generator);

    writes_.emplace_back(updated, update);

    return table_->Update(txn_, updated, *update);
  }

  template<class Random>
  void RandomSelect(Random *generator) {
    if (all_slots_->Size() == 0) return;
    storage::TupleSlot selected = *MultiThreadedTestUtil::UniformRandomElement(all_slots_, generator);
    byte *select_buffer = new byte[redo_size_];
    storage::ProjectedRow *select = storage::ProjectedRow::InitializeProjectedRow(select_buffer, all_col_ids_, layout_);
    table_->Select(txn_, selected, select);
    reads_.emplace_back(selected, select);
  }

 private:
  const storage::BlockLayout &layout_;
  storage::DataTable *table_;

  transaction::TransactionContext *txn_;
  // This will not work if GC is turned on
  common::ConcurrentVector<storage::TupleSlot> *all_slots_;
  using entry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
  std::vector<entry> writes_;
  std::vector<entry> reads_;

  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
};

TEST(LargeTransactionTest, MixedReadWrite) {
  storage::BlockStore block_store{100};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool{10000};
  std::default_random_engine generator;

  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_concurrent_txns = 8;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(max_columns, &generator);
    storage::DataTable table(&block_store, layout);
    transaction::TransactionManager txn_manager(&buffer_pool);
    common::ConcurrentVector<storage::TupleSlot> all_slots;
    std::vector<RandomWorkloadTransaction> txns;
    for (uint32_t txn_id = 0; txn_id < num_concurrent_txns; txn_id++)
      txns.emplace_back(layout, &table, &txn_manager, &all_slots);

    auto workload = [&](uint32_t id) {
      std::default_random_engine thread_generator(id);
      RandomWorkloadTransaction &txn = txns[id];
      bool aborted = false;
      auto insert = [&] { if (!aborted) txn.RandomInsert(&thread_generator); };
      auto update = [&] { if (!aborted) aborted = !txn.RandomUpdate(&thread_generator); };
      auto select = [&] { if (!aborted) txn.RandomSelect(&thread_generator); };
      MultiThreadedTestUtil::InvokeWorkloadWithDistribution({insert, update, select},
                                                            {0.1, 0.2, 0.7},
                                                            &thread_generator,
                                                            100);
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_concurrent_txns, workload);
  }
}
}  // namespace terrier
