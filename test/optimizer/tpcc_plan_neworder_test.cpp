#include <memory>
#include <string>

#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace noisepage {

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
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetTableOid(), test->tbl_district_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);
    EXPECT_EQ(update->GetOutputSchema()->GetColumns().size(), 0);

    EXPECT_EQ(update->GetSetClauses().size(), 1);
    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("d_next_o_id").Oid());
    auto expr = update->GetSetClauses()[0].second.CastManagedPointerTo<parser::OperatorExpression>();
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
    auto dve = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
    EXPECT_EQ(dve->GetColumnOid(), schema.GetColumn("d_next_o_id").Oid());
    EXPECT_EQ(expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto cve = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(cve->Peek<int64_t>(), 1);

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);

    // IdxScan OutputSchema/ColumnIds
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), schema.GetColumns().size());
    EXPECT_EQ(idx_scan->GetColumnOids().size(), schema.GetColumns().size());

    size_t idx = 0;
    std::vector<catalog::col_oid_t> oids;
    for (auto &col : schema.GetColumns()) {
      auto idx_scan_expr = idx_scan_schema->GetColumn(idx).GetExpr();
      EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
      auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
      EXPECT_EQ(idx_scan_expr_dve->GetColumnOid(), col.Oid());
      oids.emplace_back(col.Oid());
      idx++;
    }
    test->CheckOids(idx_scan->GetColumnOids(), oids);
  };

  std::string query = "UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = 1 AND D_ID = 2";
  OptimizeUpdate(query, tbl_district_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOOrder) {
  std::string query =
      "INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) "
      "VALUES (1,2,3,4,'2020-01-02',6,7)";
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
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetTableOid(), test->tbl_stock_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);
    EXPECT_EQ(update->GetOutputSchema()->GetColumns().size(), 0);

    EXPECT_EQ(update->GetSetClauses().size(), 4);
    {
      EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("s_quantity").Oid());
      EXPECT_EQ(update->GetSetClauses()[1].first, schema.GetColumn("s_ytd").Oid());
      EXPECT_EQ(update->GetSetClauses()[2].first, schema.GetColumn("s_order_cnt").Oid());
      EXPECT_EQ(update->GetSetClauses()[3].first, schema.GetColumn("s_remote_cnt").Oid());
    }

    {
      auto expr = update->GetSetClauses()[0].second.CastManagedPointerTo<parser::ConstantValueExpression>();
      EXPECT_EQ(expr->Peek<int64_t>(), 1);
    }

    {
      auto expr = update->GetSetClauses()[1].second.CastManagedPointerTo<parser::OperatorExpression>();
      EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
      auto dve = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      EXPECT_EQ(dve->GetColumnOid(), schema.GetColumn("s_ytd").Oid());
      EXPECT_EQ(expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
      auto cve = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
      EXPECT_EQ(cve->Peek<int64_t>(), 1);
    }

    {
      auto expr = update->GetSetClauses()[2].second.CastManagedPointerTo<parser::OperatorExpression>();
      EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
      auto dve = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      EXPECT_EQ(dve->GetColumnOid(), schema.GetColumn("s_order_cnt").Oid());
      EXPECT_EQ(expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
      auto cve = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
      EXPECT_EQ(cve->Peek<int64_t>(), 1);
    }

    {
      auto expr = update->GetSetClauses()[3].second.CastManagedPointerTo<parser::OperatorExpression>();
      EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
      auto dve = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      EXPECT_EQ(dve->GetColumnOid(), schema.GetColumn("s_remote_cnt").Oid());
      EXPECT_EQ(expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
      auto cve = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
      EXPECT_EQ(cve->Peek<int64_t>(), 1);
    }

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);

    // IdxScan OutputSchema/ColumnIds
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), schema.GetColumns().size());
    EXPECT_EQ(idx_scan->GetColumnOids().size(), schema.GetColumns().size());

    size_t idx = 0;
    std::vector<catalog::col_oid_t> oids;
    for (auto &col : schema.GetColumns()) {
      auto idx_scan_expr = idx_scan_schema->GetColumn(idx).GetExpr();
      EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
      auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
      EXPECT_EQ(idx_scan_expr_dve->GetColumnOid(), col.Oid());
      oids.emplace_back(col.Oid());
      idx++;
    }
    test->CheckOids(idx_scan->GetColumnOids(), oids);
  };

  std::string query =
      "UPDATE STOCK SET S_QUANTITY = 1, "
      "S_YTD = S_YTD + 1, S_ORDER_CNT = S_ORDER_CNT + 1, "
      "S_REMOTE_CNT = S_REMOTE_CNT + 1 WHERE S_I_ID = 2 AND S_W_ID = 3";
  OptimizeUpdate(query, tbl_stock_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanNewOrderTests, InsertOrderLine) {
  std::string query =
      "INSERT INTO \"ORDER LINE\" "
      "(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, "
      "OL_DIST_INFO) "
      "VALUES (1,2,3,4,5,6,'2020-01-02',7,8,'dist')";
  OptimizeInsert(query, tbl_order_line_);
}

}  // namespace noisepage
