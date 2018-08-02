#include "common/test_util.h"
#include "storage/data_table.h"
#include "storage/storage_utils.h"
#include "util/storage_test_util.h"

namespace terrier {
struct DataTableTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  std::default_random_engine generator_;
};

// Generates a random table layout and coin flip bias for an attribute being null, inserts num_inserts random tuples
// into an empty DataTable. Then, Selects the inserted TupleSlots and compares the results to the original inserted
// random tuple. Repeats for num_iterations.
TEST_F(DataTableTests, SimpleInsertSelectTest) {
  const uint16_t num_iterations = 100;
  const uint32_t num_inserts = 100;
  const uint16_t max_columns = 100;

//  std::uniform_real_distribution<> distribution(0.0, 1.0);

  for (uint16_t iteration = 0; iteration < num_iterations; ++iteration) {

//    double null_bias = distribution(generator_);

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> redo_buffers(num_inserts);
    std::vector<byte *> undo_buffers(num_inserts);
    std::vector<std::pair<storage::TupleSlot, storage::ProjectedRow *>> inserted_tuples;

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    for (uint32_t i = 0; i < num_inserts; ++i) {

      // generate a random redo ProjectedRow to Insert
      byte *redo_buffer = new byte[redo_size];
      redo_buffers[i] = redo_buffer;
      storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, redo_buffer);
      testutil::GenerateRandomRow(redo, layout, generator_, 0);

      // generate an undo DeltaRecord to populate on Insert
      byte *undo_buffer = new byte[undo_size];
      undo_buffers[i] = undo_buffer;
      storage::DeltaRecord *undo =
          storage::DeltaRecord::InitializeDeltaRecord(nullptr, VALUE_OF_EXPLICIT_TYPE(timestamp_t, uint64_t, 0), layout, col_ids, undo_buffer);

      storage::TupleSlot tuple = table.Insert(*redo, undo);

      inserted_tuples.emplace_back(tuple, redo);
    }

    EXPECT_EQ(num_inserts, inserted_tuples.size());

    std::vector<byte *> select_buffers(num_inserts);

    for (uint32_t i = 0; i < num_inserts; ++i) {
      // generate a redo ProjectedRow for Select
      byte *select_buffer = new byte[redo_size];
      select_buffers[i] = select_buffer;
      storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, select_buffer);

      table.Select(VALUE_OF_EXPLICIT_TYPE(timestamp_t, uint64_t, 1), inserted_tuples[i].first, select_row);

      EXPECT_EQ(0, std::memcmp(inserted_tuples[i].second, select_row, redo_size));
    }

    for (uint32_t i = 0; i < num_inserts; i++) {
      delete[] redo_buffers[i];
      delete[] undo_buffers[i];
      delete[] select_buffers[i];
    }
  }
}

// Generates a random table layout and coin flip bias for an attribute being null, inserts num_inserts random tuples
// into an empty DataTable with timestamp 1. Then, Selects the inserted TupleSlots with timestamp 0, and ensures that
// all NULL attributes are returned. Repeats for num_iterations.
TEST_F(DataTableTests, SimpleVersionChainTest) {
  const uint16_t num_iterations = 1000;
  const uint32_t num_inserts = 10;
  const uint16_t max_columns = 100;

//  std::uniform_real_distribution<> distribution(0.0, 1.0);

  for (uint16_t iteration = 0; iteration < num_iterations; ++iteration) {

//    double null_bias = distribution(generator_);

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> redo_buffers(num_inserts);
    std::vector<byte *> undo_buffers(num_inserts);
    std::vector<std::pair<storage::TupleSlot, storage::ProjectedRow *>> inserted_tuples;

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    for (uint32_t i = 0; i < num_inserts; ++i) {

      // generate a random redo ProjectedRow to Insert
      byte *redo_buffer = new byte[redo_size];
      redo_buffers[i] = redo_buffer;
      storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, redo_buffer);
      testutil::GenerateRandomRow(redo, layout, generator_, 0);

      // generate an undo DeltaRecord to populate on Insert
      byte *undo_buffer = new byte[undo_size];
      undo_buffers[i] = undo_buffer;
      storage::DeltaRecord *undo =
          storage::DeltaRecord::InitializeDeltaRecord(nullptr, VALUE_OF_EXPLICIT_TYPE(timestamp_t, uint64_t, 1), layout, col_ids, undo_buffer);

      storage::TupleSlot tuple = table.Insert(*redo, undo);

      inserted_tuples.emplace_back(tuple, redo);
    }

    EXPECT_EQ(num_inserts, inserted_tuples.size());

    std::vector<byte *> select_buffers(num_inserts);

    for (uint32_t i = 0; i < num_inserts; ++i) {
      // generate a redo ProjectedRow for Select
      byte *select_buffer = new byte[redo_size];
      select_buffers[i] = select_buffer;
      storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, select_buffer);

      table.Select(VALUE_OF_EXPLICIT_TYPE(timestamp_t, uint64_t, 0), inserted_tuples[i].first, select_row);

      for (uint16_t j = 0; j < select_row->NumColumns(); j++) {
        EXPECT_EQ(nullptr, select_row->AccessWithNullCheck(j));
      }
    }

    for (uint32_t i = 0; i < num_inserts; i++) {
      delete[] redo_buffers[i];
      delete[] undo_buffers[i];
      delete[] select_buffers[i];
    }
  }
}

}  // namespace terrier
