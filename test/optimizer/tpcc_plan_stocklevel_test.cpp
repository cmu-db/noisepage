#include <memory>
#include <string>

#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanStockLevelTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetDistrictOrderId) {
  std::string query = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2";
  OptimizeQuery(query, tbl_district_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetCountStock) {
  // TODO(wz2): Update
  // From OLTPBenchmark (L43-51)
  // SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT
  //   FROM ORDER-LINE, STOCK
  //  WHERE OL_W_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_O_ID < ?
  //    AND OL_O_ID >= ?
  //    AND S_W_ID = ?
  //    AND S_I_ID = OL_I_ID
  //    AND S_QUANTITY < ?
  EXPECT_TRUE(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetJoinStock) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {};

  std::string query = "SELECT S_I_ID FROM \"ORDER LINE\", STOCK WHERE OL_W_ID=1 AND S_W_ID=1";
  OptimizeQuery(query, tbl_stock_, check);
}

}  // namespace terrier
