#include <memory>
#include <string>

#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanPaymentTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateWarehouse) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    auto w_ytd_idx = -1;
    for (size_t i = 0; i < schema.GetColumns().size(); i++) {
      if (schema.GetColumns()[i].Name() == "w_ytd") {
        w_ytd_idx = static_cast<int>(i);
      }
    }

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_warehouse_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);
    EXPECT_EQ(update->GetOutputSchema()->GetColumns().size(), 0);

    EXPECT_EQ(update->GetSetClauses().size(), 1);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("w_ytd").Oid());
    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
    auto dve = expr->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), w_ytd_idx);

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDesc
    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 1);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 1);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("w_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 2);

    // IdxScan OutputSchema/ColumnIds -> match schema
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), schema.GetColumns().size());
    EXPECT_EQ(idx_scan->GetColumnIds().size(), schema.GetColumns().size());

    size_t idx = 0;
    for (auto &col : schema.GetColumns()) {
      auto idx_scan_expr = idx_scan_schema->GetColumn(idx).GetExpr();
      EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
      auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::DerivedValueExpression>();
      EXPECT_EQ(idx_scan_expr_dve->GetTupleIdx(), 0);
      EXPECT_EQ(idx_scan_expr_dve->GetValueIdx(), idx);

      EXPECT_EQ(idx_scan->GetColumnIds()[idx], col.Oid());
      idx++;
    }
  };

  std::string query = "UPDATE WAREHOUSE SET W_YTD = W_YTD + 1 WHERE W_ID = 2";
  OptimizeUpdate(query, tbl_warehouse_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetWarehouse) {
  std::string query = "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME FROM WAREHOUSE WHERE W_ID=1";
  OptimizeQuery(query, tbl_warehouse_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateDistrict) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    auto d_ytd_idx = -1;
    for (size_t i = 0; i < schema.GetColumns().size(); i++) {
      if (schema.GetColumns()[i].Name() == "d_ytd") {
        d_ytd_idx = static_cast<int>(i);
      }
    }

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_district_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);
    EXPECT_EQ(update->GetOutputSchema()->GetColumns().size(), 0);

    EXPECT_EQ(update->GetSetClauses().size(), 1);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("d_ytd").Oid());
    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
    auto dve = expr->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), d_ytd_idx);

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDesc
    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 2);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 2);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("d_w_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("d_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 3);

    // IdxScan OutputSchema/ColumnIds -> match schema
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), schema.GetColumns().size());
    EXPECT_EQ(idx_scan->GetColumnIds().size(), schema.GetColumns().size());

    size_t idx = 0;
    for (auto &col : schema.GetColumns()) {
      auto idx_scan_expr = idx_scan_schema->GetColumn(idx).GetExpr();
      EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
      auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::DerivedValueExpression>();
      EXPECT_EQ(idx_scan_expr_dve->GetTupleIdx(), 0);
      EXPECT_EQ(idx_scan_expr_dve->GetValueIdx(), idx);

      EXPECT_EQ(idx_scan->GetColumnIds()[idx], col.Oid());
      idx++;
    }
  };

  std::string query = "UPDATE DISTRICT SET D_YTD = D_YTD + 1 WHERE D_W_ID = 2 AND D_ID = 3";
  OptimizeUpdate(query, tbl_district_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetDistrict) {
  std::string query =
      "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2";
  OptimizeQuery(query, tbl_district_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetCustomer) {
  std::string query =
      "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, "
      "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, "
      "C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE "
      "FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, GetCustomerCData) {
  std::string query = "SELECT C_DATA FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, UpdateCustomerBalance) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_customer_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);
    EXPECT_EQ(update->GetOutputSchema()->GetColumns().size(), 0);

    EXPECT_EQ(update->GetSetClauses().size(), 4);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("c_balance").Oid());
    EXPECT_EQ(update->GetSetClauses()[1].first, schema.GetColumn("c_ytd_payment").Oid());
    EXPECT_EQ(update->GetSetClauses()[2].first, schema.GetColumn("c_payment_cnt").Oid());
    EXPECT_EQ(update->GetSetClauses()[3].first, schema.GetColumn("c_data").Oid());
    auto set0 = update->GetSetClauses()[0].second.CastManagedPointerTo<parser::ConstantValueExpression>();
    auto set1 = update->GetSetClauses()[1].second.CastManagedPointerTo<parser::ConstantValueExpression>();
    auto set2 = update->GetSetClauses()[2].second.CastManagedPointerTo<parser::ConstantValueExpression>();
    auto set3 = update->GetSetClauses()[3].second.CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(set0->GetValue()), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(set1->GetValue()), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(set2->GetValue()), 3);
    EXPECT_EQ(type::TransientValuePeeker::PeekVarChar(set3->GetValue()), "4");

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDesc
    // Based on TrivialCostModel, secondary index gets picked
    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 2);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    // EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 2);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("c_w_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("c_d_id").Oid());
    // EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("c_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    // EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[2]), 3);

    // IdxScan OutputSchema/ColumnIds -> match schema
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), schema.GetColumns().size());
    EXPECT_EQ(idx_scan->GetColumnIds().size(), schema.GetColumns().size());

    size_t idx = 0;
    for (auto &col : schema.GetColumns()) {
      auto idx_scan_expr = idx_scan_schema->GetColumn(idx).GetExpr();
      EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
      auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::DerivedValueExpression>();
      EXPECT_EQ(idx_scan_expr_dve->GetTupleIdx(), 0);
      EXPECT_EQ(idx_scan_expr_dve->GetValueIdx(), idx);

      EXPECT_EQ(idx_scan->GetColumnIds()[idx], col.Oid());
      idx++;
    }
  };

  std::string query =
      "UPDATE CUSTOMER SET C_BALANCE = 1, C_YTD_PAYMENT = 2,"
      "C_PAYMENT_CNT = 3, C_DATA = '4' WHERE C_W_ID = 1 AND C_D_ID = 2 AND C_ID = 3";
  OptimizeUpdate(query, tbl_customer_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, InsertHistory) {
  std::string query =
      "INSERT INTO HISTORY "
      "(H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
      "VALUES (1,2,3,4,5,0,7,'data')";
  OptimizeInsert(query, tbl_history_);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanPaymentTests, CustomerByName) {
  std::string query =
      "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
      "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, "
      "C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE "
      "FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_LAST='page' "
      "ORDER BY C_FIRST";
  OptimizeQuery(query, tbl_customer_, TpccPlanTest::CheckIndexScan);
}

}  // namespace terrier
