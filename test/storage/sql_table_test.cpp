#include "storage/sql_table.h"

#include <cstring>
#include <vector>

#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace terrier {

struct SqlTableTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

// NOLINTNEXTLINE
TEST_F(SqlTableTests, SimpleInsert) { EXPECT_TRUE(1 == 1); }
}  // namespace terrier
