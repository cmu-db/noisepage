#include "util/tpcc/tpcc_plan_test.h"
#include "util/test_harness.h"

namespace terrier {

struct TpccPlanDeliveryTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetOrderId) {
  // From OLTPBenchmark (L38-43)
  // SELECT NO_O_ID FROM NEW-ORDER
  //  WHERE NO_D_ID = ?
  //    AND NO_W_ID = ?
  //  ORDER BY NO_O_ID ASC
  //  LIMIT 1
  EXPECT_TRUE(false);
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
  // From OLTPBenchmark (L51-55)
  // SELECT O_C_ID FROM ORDER
  //  WHERE O_ID = ?
  //    AND O_D_ID = ?
  //    AND O_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateCarrierId) {
  // From OLTPBenchmark (L57-62)
  // UPDATE ORDER
  //    SET O_CARRIER_ID = ?
  //  WHERE O_ID = ?
  //    AND O_D_ID = ?
  //    AND O_W_ID = ?
  EXPECT_TRUE(false);
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
