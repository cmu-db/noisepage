/*
#include <string>

#include "util/test_harness.h"
#include "util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanDeliveryTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetOrderId) {
  std::string query = "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = 1 AND NO_W_ID = 2 ORDER BY NO_O_ID LIMIT 1";
  OptimizeQuery(query, "new_order", tbl_new_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryDeleteNewOrder) {
  // From OLTPBenchmark (L45-49)
  // DELETE FROM NEW-ORDER
  //  WHERE NO_O_ID = ?
  //    AND NO_D_ID = ?
  //    AND NO_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetCustomerId) {
  std::string query = "SELECT O_C_ID FROM \"ORDER\" WHERE O_ID = 1 AND (O_D_ID = 2 AND O_W_ID = 3)";
  OptimizeQuery(query, "order", tbl_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateCarrierId) {
  std::string query = "UPDATE \"ORDER\" SET O_CARRIER_ID = 1 WHERE O_ID = 1 AND O_D_ID = 2 AND O_W_ID = 3";
  OptimizeUpdate(query, "order", tbl_order_);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateDeliveryDate) {
  // From OLTPBenchmark (L65-69)
  // UPDATE ORDER-LINE
  //    SET OL_DELIVERY_D = ?
  //  WHERE OL_O_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliverySumOrderAmount) {
  // From OLTPBenchmark (L73-76)
  // SELECT SUM(OL_AMOUNT) AS OL_TOTAL
  //   FROM ORDERLINE
  //  WHERE OL_O_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, UpdateCustomBalanceDeliveryCount) {
  // From OLTPBenchmark (L78-84)
  // UPDATE CUSTOMER
  //    SET C_BALANCE = C_BALANCE + ?
  //        C_DELIVERY_CNT = C_DELIVERY_CNT + 1
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

}  // namespace terrier
*/
