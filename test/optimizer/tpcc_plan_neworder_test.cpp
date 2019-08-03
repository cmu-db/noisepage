#include "util/tpcc/tpcc_plan_test.h"
#include "util/test_harness.h"

namespace terrier {

struct TpccPlanNewOrderTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetCustomer) {
  // From OLTPBenchmark (L38-42)
  // SELECT C_DISCOUNT, C_LAST, C_CREDIT
  //   FROM CUSTOMER
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetWarehouse) {
  // From OLTPBenchmark (L45-47)
  // SELECT W_TAX
  //   FROM WAREHOUSE
  //  WHERE W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetDistrict) {
  // From OLTPBenchmark (L50-52)
  // SELECT D_NEXT_O_ID, D_TAX
  //   FROM DISTRICT
  //  WHERE D_W_ID = ?
  //    AND D_ID = ?
  //  FOR UPDATE
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertNewOrder) {
  // From OLTPBenchmark (L55-57)
  // INSERT INTO NEW-ORDER
  // (NO_O_ID, NO_D_ID, NO_W_ID)
  // VALUES (?, ?, ?)
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, UpdateDistrict) {
  // From OLTPBenchmark (L60-63)
  // UDPATE DISTRICT
  //    SET D_NEXT_O_ID = D_NEXT_O_ID + 1
  //  WHERE D_W_ID = ?
  //    AND D_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOOrder) {
  // From OLTPBenchmark (L66-68)
  // INSERT INTO ORDER
  // (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
  // VALUES (?, ?, ?, ?, ?, ?, ?)
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetItem) {
  // From OLTPBenchmark (L71-73)
  // SELECT I_PRICE, I_NAME, I_DATA
  //   FROM ITEM
  //  WHERE I_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetStock) {
  // From OLTPBenchmark (L76-80)
  // SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05,
  //        S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10
  //   FROM STOCK
  //  WHERE S_I_ID = ?
  //    AND S_W_ID = ?
  // FOR UPDATE
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, UpdateStock) {
  // From OLTPBenchmark (L83-89)
  // UPDATE STOCK
  //    SET S_QUANTITY = ?
  //        S_YTD = S_YTD + ?
  //        S_ORDER_CNT = S_ORDER_CNT + 1
  //        S_REMOTE_CNT = S_REMOTE_CNT + ?
  //  WHERE S_I_ID = ?
  //    AND S_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOrderLine) {
  // From OLTPBenchmark (L92-94)
  // INSERT INTO ORDER-LINE
  // (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
  // VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  EXPECT_TRUE(false);
}

}  // namespace terrier
