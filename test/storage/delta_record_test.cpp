#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {

struct DeltaRecordTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_int_distribution<uint64_t> timestamp_dist_{0, ULONG_MAX};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{10, 10};

 protected:
  void SetUp() override { raw_block_ = block_store_.Get(); }

  void TearDown() override { block_store_.Release(raw_block_); }
};

// Generates a list of UndoRecords and chain them together. Access from the beginning and see if we can access all
// UndoRecords. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(DeltaRecordTests, UndoChainAccess) {
  uint32_t num_iterations = 10;
  uint32_t max_chain_size = 20;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    std::vector<storage::UndoRecord *> record_list;
    std::uniform_int_distribution<> size_dist(1, max_chain_size);
    uint32_t chain_size = size_dist(generator_);
    for (uint32_t i = 0; i < chain_size; ++i) {
      // get random layout
      storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);
      storage::TupleAccessStrategy tested(layout);
      std::memset(reinterpret_cast<void *>(raw_block_), 0, sizeof(storage::RawBlock));
      tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

      // get data table
      storage::DataTable data_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                                    storage::layout_version_t(0));

      // get tuple slot
      storage::TupleSlot slot;
      EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

      // compute the size of the buffer
      const std::vector<storage::col_id_t> col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, &generator_);
      storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, col_ids);
      transaction::timestamp_t time = static_cast<transaction::timestamp_t>(timestamp_dist_(generator_));
      auto *record_buffer = common::AllocationUtil::AllocateAligned(storage::UndoRecord::Size(initializer));
      storage::UndoRecord *record =
          storage::UndoRecord::InitializeUpdate(record_buffer, time, slot, &data_table, initializer);
      // Chain the records
      if (i != 0) record_list.back()->Next() = record;
      record_list.push_back(record);
    }

    for (uint32_t i = 0; i < record_list.size() - 1; i++) EXPECT_EQ(record_list[i]->Next(), record_list[i + 1]);

    for (auto record : record_list) delete[] reinterpret_cast<byte *>(record);
  }
}

// Generate UndoRecords using ProjectedRows and get ProjectedRows back from UndoRecords to see if you get the same
// ProjectedRows back. Repeat for num_iterations.
// NOLINTNEXTLINE
TEST_F(DeltaRecordTests, UndoGetProjectedRow) {
  uint32_t num_iterations = 50;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);
    storage::TupleAccessStrategy tested(layout);
    std::memset(reinterpret_cast<void *>(raw_block_), 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    // generate a random projectedRow
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, update_col_ids);
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *redo = initializer.InitializeRow(redo_buffer);
    // we don't need to populate projected row since we only copying the layout when we create a UndoRecord using
    // projected row

    // get data table
    storage::DataTable data_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                                  storage::layout_version_t(0));

    // get tuple slot
    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

    // compute the size of the buffer
    uint32_t size = storage::UndoRecord::Size(*redo);
    transaction::timestamp_t time = static_cast<transaction::timestamp_t>(timestamp_dist_(generator_));
    auto *record_buffer = common::AllocationUtil::AllocateAligned(size);
    storage::UndoRecord *record = storage::UndoRecord::InitializeUpdate(record_buffer, time, slot, &data_table, *redo);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(layout, record->Delta(), redo));
    delete[] redo_buffer;
    delete[] record_buffer;
  }
}
}  // namespace noisepage
