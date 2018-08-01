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

TEST_F(DataTableTests, SimpleCorrectnessTest) {
  storage::BlockLayout layout = testutil::RandomLayout(generator_);
  storage::DataTable table(block_store_, layout);
}

}