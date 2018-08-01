#include "common/test_util.h"
#include "storage/data_table.h"
#include "storage/storage_utils.h"
#include "util/storage_test_util.h"

namespace terrier {
struct DataTableTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  std::default_random_engine generator_;
};

TEST_F(DataTableTests, SimpleInsertTest) {
  uint32_t num_inserts = 1000;
  uint16_t max_columns = 100;
  uint16_t num_repetitions = 10;

  for (uint16_t iteration = 0; iteration < num_repetitions; ++iteration) {

    storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
    std::cout << layout.num_cols_ << std::endl;
    storage::DataTable table(block_store_, layout);

    std::vector<byte *> insert_redos(num_inserts);
    std::vector<byte *> insert_undos(num_inserts);
    std::vector<std::pair<storage::TupleSlot, storage::ProjectedRow *>> inserted_tuples;

    std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    uint32_t undo_size = storage::DeltaRecord::Size(layout, col_ids);

    for (uint32_t i = 0; i < num_inserts; ++i) {

      // generate a random redo ProjectedRow to Insert
      byte *redo_buffer = new byte[redo_size];
      insert_redos[i] = redo_buffer;
      storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, redo_buffer);
      testutil::GenerateRandomRow(redo, layout, generator_, 0);

      // generate an undo DeltaRecord to populate on Insert
      byte *undo_buffer = new byte[undo_size];
      insert_undos[i] = undo_buffer;
      storage::DeltaRecord *undo =
          storage::DeltaRecord::InitializeDeltaRecord(nullptr, VALUE_OF(timestamp_t, 0ull), layout, col_ids, undo_buffer);

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

      table.Select(VALUE_OF(timestamp_t, 1ull), inserted_tuples[i].first, select_row);

      storage::ProjectedRow *inserted_row = inserted_tuples[i].second;

      EXPECT_EQ(0, std::memcmp(inserted_row, select_row, redo_size));
    }

    for (uint32_t i = 0; i < num_inserts; i++) {
      delete[] insert_redos[i];
      delete[] insert_undos[i];
      delete[] select_buffers[i];
    }
  }
}

}