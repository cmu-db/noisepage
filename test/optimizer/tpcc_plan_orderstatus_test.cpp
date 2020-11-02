#include <string>

#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace noisepage {

struct TpccPlanOrderStatusTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetNewestOrd) {
  std::string query =
      "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM \"ORDER\" WHERE O_W_ID=1 AND O_D_ID=2 AND O_C_ID=3 ORDER BY O_ID DESC "
      "LIMIT 1";
  OptimizeQuery(query, tbl_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetOrderLine) {
  std::string query =
      "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D "
      "FROM \"ORDER LINE\" WHERE OL_O_ID=1 AND OL_D_ID=2 AND OL_W_ID=3";
  OptimizeQuery(query, tbl_order_line_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, GetCustomer) {
  std::string query =
      "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, "
      "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, "
      "C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE "
      "FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanOrderStatusTests, CustomerByName) {
  std::string query =
      "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
      "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, "
      "C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE "
      "FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_LAST='page' "
      "ORDER BY C_FIRST";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

}  // namespace noisepage
