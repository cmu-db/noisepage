#include "common/test_util.h"
#include "storage/data_table.h"
#include "storage/storage_utils.h"
#include "util/storage_test_util.h"

namespace terrier {
struct DataTableTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  std::default_random_engine generator_;

 protected:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(DataTableTests, SimpleInsertTest) {
  uint16_t num_inserts = 1000;
  uint16_t max_columns = 100;

  storage::BlockLayout layout = testutil::RandomLayout(generator_, max_columns);
  storage::DataTable table(block_store_, layout);

  std::vector<byte *> insert_redos(num_inserts);
  std::vector<byte *> insert_undos(num_inserts);

  std::vector<uint16_t> col_ids = testutil::ProjectionListAllColumns(layout);

  uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);

  for (uint16_t i = 0; i < num_inserts; ++i) {
    byte *redo_buffer = new byte[redo_size];
    insert_redos.push_back(redo_buffer);

    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, redo_buffer);

    testutil::GenerateRandomRow(redo, layout, generator_, 0);
  }

  for (auto i : insert_redos) {
    delete[] i;
  }

  for (auto i : insert_undos) {
    delete[] i;
  }
}

}