#include <memory>
#include <string>

#include "planner/plannodes/update_plan_node.h"
#include "util/test_harness.h"
#include "util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanDeliveryTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetOrderId) {
  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1 AND NO_W_ID = 2 ORDER BY NO_O_ID LIMIT 1";
  OptimizeQuery(query, tbl_new_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryDeleteNewOrder) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {};

  std::string query = "DELETE FROM \"NEW ORDER\" WHERE NO_O_ID = 1 AND NO_D_ID = 2 AND NO_W_ID = 3";
  OptimizeDelete(query, tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetCustomerId) {
  std::string query = "SELECT O_C_ID FROM \"ORDER\" WHERE O_ID = 1 AND (O_D_ID = 2 AND O_W_ID = 3)";
  OptimizeQuery(query, tbl_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateCarrierId) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);

    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_order_);

    auto &schema = test->accessor_->GetSchema(test->tbl_order_);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("o_carrier_id").Oid());

    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto cve = expr.CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "UPDATE \"ORDER\" SET O_CARRIER_ID = 1 WHERE O_ID = 1 AND O_D_ID = 2 AND O_W_ID = 3";
  OptimizeUpdate(query, tbl_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateDeliveryDate) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);

    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_order_line_);

    auto &schema = test->accessor_->GetSchema(test->tbl_order_line_);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("ol_delivery_d").Oid());

    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto cve = expr.CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "UPDATE \"ORDER LINE\" SET OL_DELIVERY_D = 1 WHERE OL_O_ID = 1 AND OL_D_ID = 2 AND OL_W_ID = 3";
  OptimizeUpdate(query, tbl_order_line_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliverySumOrderAmount) {
  // TODO(wz2): Test Plan
  // From OLTPBenchmark (L73-76)
  // SELECT SUM(OL_AMOUNT) AS OL_TOTAL
  //   FROM ORDERLINE
  //  WHERE OL_O_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_W_ID = ?
  EXPECT_TRUE(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, UpdateCustomBalanceDeliveryCount) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {};

  std::string query =
      "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE+1, C_DELIVERY_CNT=C_DELIVERY_CNT+1"
      "WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeUpdate(query, tbl_order_line_, check);
}

}  // namespace terrier
