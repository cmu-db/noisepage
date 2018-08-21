#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "storage/storage_defs.h"
#include "util/test_harness.h"

namespace terrier {

struct DeltaRecordTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_int_distribution<uint64_t> timestamp_dist_{0, ULONG_MAX};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};
  std::vector<byte *> loose_pointers_;

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
    raw_block_ = block_store_.Get();
  }

  void TearDown() override {
    block_store_.Release(raw_block_);
    for (auto entry : loose_pointers_) {
      delete[] entry;
    }
    TerrierTest::TearDown();
  }
};


// Generates a list of UndoRecords and chain them together. Access from the beginning and see if we can access all
// UndoRecords. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(DeltaRecordTests, UndoChainAccess) {
  uint32_t num_iterations = 10;
  uint32_t max_chain_size = 100;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    std::vector<storage::UndoRecord *> record_list;
    std::uniform_int_distribution<> size_dist(1, max_chain_size);
    uint32_t chain_size = size_dist(generator_);
    for (uint32_t i = 0; i < chain_size; ++i) {
      // get random layout
      storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);
      storage::TupleAccessStrategy tested(layout);
      PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
      tested.InitializeRawBlock(raw_block_, layout_version_t(0));

      // get data table
      storage::DataTable data_table(&block_store_, layout);

      // get tuple slot
      storage::TupleSlot slot;
      EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

      // compute the size of the buffer
      const std::vector<uint16_t> col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, &generator_);
      uint32_t size = storage::UndoRecord::Size(layout, col_ids);
      timestamp_t time = static_cast<timestamp_t >(timestamp_dist_(generator_));
      auto *record_buffer = new byte[size];
      loose_pointers_.push_back(record_buffer);
      storage::UndoRecord *record = storage::UndoRecord::InitializeRecord(record_buffer,
                                                                          time,
                                                                          slot,
                                                                          &data_table,
                                                                          layout,
                                                                          col_ids);
      // Chain the records
      if (i != 0)
        record_list.back()->Next() = record;
      record_list.push_back(record);
    }

    for (uint32_t i = 0; i < record_list.size() - 1; i++)
      EXPECT_EQ(record_list[i]->Next(), record_list[i+1]);
  }
}


// Generate UndoRecords using ProjectedRows and get ProjectedRows back from UndoRecords to see if you get the same
// ProjectedRows back. Repeat for num_iterations.
// NOLINTNEXTLINE
TEST_F(DeltaRecordTests, UndoGetProjectedRow){
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);
    storage::TupleAccessStrategy tested(layout);
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    // generate a random projectedRow
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    auto *redo_buffer = new byte[storage::ProjectedRow::Size(layout, update_col_ids)];
    storage::ProjectedRow *redo =
        storage::ProjectedRow::InitializeProjectedRow(redo_buffer, update_col_ids, layout);
    // we don't need to populate projected row since we only copying the layout when we create a UndoRecord using
    // projected row
    loose_pointers_.push_back(redo_buffer);

    // get data table
    storage::DataTable data_table(&block_store_, layout);

    // get tuple slot
    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

    // compute the size of the buffer
    uint32_t size = storage::UndoRecord::Size(*redo);
    timestamp_t time = static_cast<timestamp_t >(timestamp_dist_(generator_));
    auto *record_buffer = new byte[size];
    loose_pointers_.push_back(record_buffer);
    storage::UndoRecord *record = storage::UndoRecord::InitializeRecord(record_buffer,
                                                                        time,
                                                                        slot,
                                                                        &data_table,
                                                                        *redo);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(layout, record->Delta(), redo));
  }
}
}  // namespace terrier
