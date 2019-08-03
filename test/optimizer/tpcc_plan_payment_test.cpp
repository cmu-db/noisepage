#include "util/tpcc/tpcc_plan_test.h"
#include "util/test_harness.h"

namespace terrier {

struct TpccPlanPaymentTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateWarehouse) {
  // OLTPBenchmark 40-42
  // UPDATE WAREHOUSE
  //    SET W_YTD = W_YTD + ?
  //  WHERE W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetWarehouse) {
  // OLTPBenchmark 45-47
  // SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME
  //   FROM WAREHOUSE
  //  WHERE W_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateDistrict) {
  // OLTPbenchmark 50-53
  // UPDATE DISTRICT
  //    SET D_YTD = D_YTD + ?
  //  WHERE D_W_ID = ?
  //    AND D_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetDistrict) {
  // OLTPBenchmark 56-59
  // SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME
  //   FROM DISTRICT
  //  WHERE D_W_ID = ?
  //    AND D_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetCustomer) {
  // OLTPBenchmark 62-68
  // SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2
  //        C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM
  //        C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE
  //   FROM CUSTOMER
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetCustomerCData) {
  // OLTPBenchmark 71-75
  // SELECT C_DATA
  //   FROM CUSTOMER
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateCustomerBalance) {
  // OLTPBenchmark 78-85
  // UPDATE CUSTOMER
  //    SET C_BALANCE = ?
  //        C_YTD_PAYMENT = ?
  //        C_PAYMENT_CNT = ?
  //        C_DATA = ?
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateCustBal) {
  // OLTPBenchmark 88-94
  // UPDATE CUSTOMER
  //    SET C_BALANCE = ?
  //        C_YTD_PAYMENT = ?
  //        C_PAYMENT_CNT = ?
  //  WHERE C_W_ID = ?
  //    AND C_D_ID = ?
  //    AND C_ID = ?
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, InsertHistory) {
  // OLTPBenchmark 97-99
  // INSERT INTO HISTORY
  // (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)
  // VALUES (?,?,?,?,?,?,?,?)
  EXPECT_TRUE(false);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, CustomerByName) {
  // OLTPBenchmark 102-109
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
