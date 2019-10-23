#include <string>

#include "util/test_harness.h"
#include "util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanNewOrderTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetCustomer) {
  std::string query = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetWarehouse) {
  std::string query = "SELECT W_TAX FROM WAREHOUSE WHERE W_ID=1";
  OptimizeQuery(query, tbl_warehouse_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetDistrict) {
  std::string query = "SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2";
  OptimizeQuery(query, tbl_district_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertNewOrder) {
  std::string query = "INSERT INTO \"NEW ORDER\" (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (1,2,3)";
  OptimizeInsert(query, tbl_new_order_);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, UpdateDistrict) {
  // TODO(wz2): Enable plan
  // From OLTPBenchmark (L60-63)
  // UDPATE DISTRICT
  //    SET D_NEXT_O_ID = D_NEXT_O_ID + 1
  //  WHERE D_W_ID = ?
  //    AND D_ID = ?
  EXPECT_TRUE(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOOrder) {
  std::string query =
      "INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) "
      "VALUES (1,2,3,4,0,6,7)";
  OptimizeInsert(query, tbl_order_);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetItem) {
  std::string query = "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID=1";
  OptimizeQuery(query, tbl_item_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, GetStock) {
  std::string query =
      "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05,"
      "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 "
      "FROM STOCK WHERE S_I_ID=1 AND S_W_ID=2";
  OptimizeQuery(query, tbl_stock_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, UpdateStock) {
  // TODO(wz2): Enable plan
  // From OLTPBenchmark (L83-89)
  // UPDATE STOCK
  //    SET S_QUANTITY = ?
  //        S_YTD = S_YTD + ?
  //        S_ORDER_CNT = S_ORDER_CNT + 1
  //        S_REMOTE_CNT = S_REMOTE_CNT + ?
  //  WHERE S_I_ID = ?
  //    AND S_W_ID = ?
  EXPECT_TRUE(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOrderLine) {
  std::string query =
      "INSERT INTO \"ORDER LINE\" "
      "(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) "
      "VALUES (1,2,3,4,5,6,7,8,'dist')";
  OptimizeInsert(query, tbl_order_line_);
}

}  // namespace terrier
