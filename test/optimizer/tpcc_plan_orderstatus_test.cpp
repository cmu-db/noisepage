#include "util/tpcc/tpcc_plan_test.h"
#include "util/test_harness.h"

namespace terrier {

struct TpccPlanOrderStatusTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetNewestOrd) {
  // From OLTPBenchmark (L40-45)
  // SELECT O_ID, O_CARRIER_ID, O_ENTRY_D
  //  WHERE O_W_ID = ?
  //    AND O_D_ID = ?
  //    AND O_C_ID = ?
  //  ORDER BY O_ID DESC LIMIT 1
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetOrderLine) {
  // From OLTPBenchmark (L48-52)
  // SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
  //   FROM ORDER-LINE
  //  WHERE OL_O_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetCustomer) {
  // From OLTPBenchmark (L55-61)
  // SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
  //        C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LM,
  //        C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE
  //   FROM CUSTOMER
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, CustomerByName) {
  // From OLTPBenchmark (L64-71)
  // SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY,
  //        C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
  //        C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE
  //   FROM CUSTOMER
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_LAST = ?
  //  ORDER BY C_FIRST
  EXPECT_TRUE(false);
}

}  // namespace terrier
