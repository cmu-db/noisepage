/*
#include <string>

#include "util/test_harness.h"
#include "util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanStockLevelTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetDistrictOrderId) {
  std::string query = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2";
  OptimizeQuery(query, "district", tbl_district_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetCountStock) {
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
  EXPECT_TRUE(false);
}

}  // namespace terrier
*/
