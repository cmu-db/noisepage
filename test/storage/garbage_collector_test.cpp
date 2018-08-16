#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {
// Not thread-safe
class GarbageCollectorDataTableTestObject {
 public:
  template<class Random>
  GarbageCollectorDataTableTestObject(storage::BlockStore *block_store,
                                      const uint16_t max_col,
                                      Random *generator)
      : layout_(StorageTestUtil::RandomLayout(max_col, generator)),
        table_(block_store, layout_) {}

  ~GarbageCollectorDataTableTestObject() {
    for (auto ptr : loose_pointers_)
      delete[] ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  template<class Random>
  storage::ProjectedRow *GenerateRandomTuple(Random *generator) {
    auto *buffer = new byte[redo_size_];
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);
    return redo;
  }

  template<class Random>
  storage::ProjectedRow *GenerateRandomUpdate(Random *generator) {
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    auto *buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);
    return update;
  }

  storage::ProjectedRow *GenerateVersionFromUpdate(const storage::ProjectedRow &delta,
                                                   const storage::ProjectedRow &previous) {
    auto *buffer = new byte[redo_size_];
    loose_pointers_.push_back(buffer);
    // Copy previous version
    PELOTON_MEMCPY(buffer, &previous, redo_size_);
    auto *version = reinterpret_cast<storage::ProjectedRow *>(buffer);
    std::unordered_map<uint16_t, uint16_t> col_to_projection_list_index;
    for (uint16_t i = 0; i < version->NumColumns(); i++)
      col_to_projection_list_index.emplace(version->ColumnIds()[i], i);
    storage::StorageUtil::ApplyDelta(layout_, delta, version, col_to_projection_list_index);
    return version;
  }

  storage::ProjectedRow *SelectIntoBuffer(transaction::TransactionContext *const txn,
                                          const storage::TupleSlot slot,
                                          const std::vector<uint16_t> &col_ids) {
    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer_, col_ids, layout_);
    table_.Select(txn, slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  // These always over-provision in the case of partial selects or deltas, which is fine.
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};

struct GarbageCollectorTests : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{100};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 5;
  const uint16_t max_columns_ = 100;
};

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, BasicTest) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    EXPECT_EQ(1, gc.RunGC().second);
    EXPECT_EQ(1, gc.RunGC().first);

  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, BasicThreadTest) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    gc.StartGCThread();
    EXPECT_TRUE(gc.ThreadRunning());

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    gc.StopGCThread();
    EXPECT_FALSE(gc.ThreadRunning());

  }
}

}  // namespace terrier
