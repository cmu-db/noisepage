#include <memory>
#include <string>
#include <vector>

#include "parser/expression/aggregate_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanDeliveryTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryGetOrderId) {
  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1 AND NO_W_ID = 2 ORDER BY NO_O_ID LIMIT 1";
  OptimizeQuery(query, tbl_new_order_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryDeleteNewOrder) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    auto no_o_id_idx = -1;
    auto no_d_id_idx = -1;
    auto no_w_id_idx = -1;
    for (size_t i = 0; i < schema.GetColumns().size(); i++) {
      if (schema.GetColumns()[i].Name() == "no_o_id") {
        no_o_id_idx = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "no_d_id") {
        no_d_id_idx = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "no_w_id") {
        no_w_id_idx = static_cast<int>(i);
      }
    }

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::DELETE);
    auto del_plan = reinterpret_cast<planner::DeletePlanNode *>(plan.get());
    EXPECT_EQ(del_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(del_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(del_plan->GetTableOid(), tbl_oid);
    EXPECT_EQ(del_plan->GetOutputSchema()->GetColumns().size(), 0);

    // Idx Scan, full output schema
    EXPECT_EQ(del_plan->GetChildren().size(), 1);
    EXPECT_EQ(del_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(del_plan->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDesc
    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 3);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 3);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("no_d_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("no_w_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 3);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[2]), 3);

    // Check scan predicate binds tuples correctly
    auto scan_pred = idx_scan->GetScanPredicate();
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::CONJUNCTION_AND);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::CONJUNCTION_AND);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);

    auto pred_left = scan_pred->GetChild(0).CastManagedPointerTo<parser::ConjunctionExpression>();
    EXPECT_EQ(pred_left->GetChild(0)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(pred_left->GetChild(1)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    auto pll = pred_left->GetChild(0).CastManagedPointerTo<parser::ComparisonExpression>();
    auto plr = pred_left->GetChild(1).CastManagedPointerTo<parser::ComparisonExpression>();

    auto plll = pll->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto pllr = pll->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(plll->GetTupleIdx(), 0);
    EXPECT_EQ(plll->GetValueIdx(), no_o_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(pllr->GetValue()), 1);
    auto plrl = plr->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto plrr = plr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(plrl->GetTupleIdx(), 0);
    EXPECT_EQ(plrl->GetValueIdx(), no_d_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(plrr->GetValue()), 2);

    auto pred_right = scan_pred->GetChild(1).CastManagedPointerTo<parser::ComparisonExpression>();
    EXPECT_EQ(pred_right->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(pred_right->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto prl = pred_right->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto prr = pred_right->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(prl->GetTupleIdx(), 0);
    EXPECT_EQ(prl->GetValueIdx(), no_w_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(prr->GetValue()), 3);

    // IdxScan OutputSchema/ColumnIds
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
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_order_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);

    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("o_carrier_id").Oid());
    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto cve = expr.CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);

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
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 3);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 3);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("o_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("o_d_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("o_w_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 3);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[2]), 3);

    // IdxScan OutputSchema/ColumnIds
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

  std::string query = "UPDATE \"ORDER\" SET O_CARRIER_ID = 1 WHERE O_ID = 1 AND O_D_ID = 2 AND O_W_ID = 3";
  OptimizeUpdate(query, tbl_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliveryUpdateDeliveryDate) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_order_line_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);

    EXPECT_EQ(update->GetSetClauses()[0].first, schema.GetColumn("ol_delivery_d").Oid());
    auto expr = update->GetSetClauses()[0].second;
    EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto cve = expr.CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);

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
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 3);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);

    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 3);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("ol_o_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("ol_d_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("ol_w_id").Oid());

    EXPECT_EQ(idx_desc.GetValueList().size(), 3);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[2]), 3);

    // IdxScan OutputSchema/ColumnIds
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

  std::string query = "UPDATE \"ORDER LINE\" SET OL_DELIVERY_D = 1 WHERE OL_O_ID = 1 AND OL_D_ID = 2 AND OL_W_ID = 3";
  OptimizeUpdate(query, tbl_order_line_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, DeliverySumOrderAmount) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    auto ol_amount_idx = -1;
    auto ol_o_id_idx = -1;
    auto ol_d_id_idx = -1;
    auto ol_w_id_idx = -1;
    for (size_t i = 0; i < schema.GetColumns().size(); i++) {
      if (schema.GetColumns()[i].Name() == "ol_amount") {
        ol_amount_idx = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "ol_o_id") {
        ol_o_id_idx = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "ol_d_id") {
        ol_d_id_idx = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "ol_w_id") {
        ol_w_id_idx = static_cast<int>(i);
      }
    }

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::AGGREGATE);
    auto aggr = reinterpret_cast<planner::AggregatePlanNode *>(plan.get());
    EXPECT_EQ(aggr->GetAggregateStrategyType(), planner::AggregateStrategyType::PLAIN);
    EXPECT_EQ(aggr->GetHavingClausePredicate().Get(), nullptr);
    EXPECT_EQ(aggr->GetGroupByTerms().size(), 0);

    auto aggr_schema = aggr->GetOutputSchema();
    EXPECT_EQ(aggr_schema->GetColumns().size(), 1);
    auto aggr_expr = aggr_schema->GetColumn(0).GetExpr();
    EXPECT_EQ(aggr_expr->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    auto aggr_dve = aggr_expr.CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(aggr_dve->GetTupleIdx(), 1);
    EXPECT_EQ(aggr_dve->GetValueIdx(), 0);

    // Check AggregateTerms
    EXPECT_EQ(aggr->GetAggregateTerms().size(), 1);
    auto aggr_term = aggr->GetAggregateTerms()[0];
    EXPECT_EQ(aggr_term->IsDistinct(), false);
    EXPECT_EQ(aggr_term->GetExpressionType(), parser::ExpressionType::AGGREGATE_SUM);
    EXPECT_EQ(aggr_term->GetChildrenSize(), 1);
    auto aggr_term_dve = aggr_term->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(aggr_term_dve->GetTupleIdx(), 0);
    EXPECT_EQ(aggr_term_dve->GetValueIdx(), 0);

    // Check Child
    EXPECT_EQ(aggr->GetChildren().size(), 1);
    EXPECT_EQ(aggr->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(aggr->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), false);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(idx_scan->GetColumnIds().size(), 1);
    EXPECT_EQ(idx_scan->GetColumnIds()[0], schema.GetColumn("ol_amount").Oid());

    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 3);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 3);
    EXPECT_EQ(idx_desc.GetValueList().size(), 3);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("ol_o_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("ol_d_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("ol_w_id").Oid());
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[2]), 3);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);

    // Check scan predicate binds tuples correctly
    auto scan_pred = idx_scan->GetScanPredicate();
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::CONJUNCTION_AND);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::CONJUNCTION_AND);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);

    auto pred_left = scan_pred->GetChild(0).CastManagedPointerTo<parser::ConjunctionExpression>();
    EXPECT_EQ(pred_left->GetChild(0)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(pred_left->GetChild(1)->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    auto pll = pred_left->GetChild(0).CastManagedPointerTo<parser::ComparisonExpression>();
    auto plr = pred_left->GetChild(1).CastManagedPointerTo<parser::ComparisonExpression>();

    auto plll = pll->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto pllr = pll->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(plll->GetTupleIdx(), 0);
    EXPECT_EQ(plll->GetValueIdx(), ol_o_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(pllr->GetValue()), 1);
    auto plrl = plr->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto plrr = plr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(plrl->GetTupleIdx(), 0);
    EXPECT_EQ(plrl->GetValueIdx(), ol_d_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(plrr->GetValue()), 2);

    auto pred_right = scan_pred->GetChild(1).CastManagedPointerTo<parser::ComparisonExpression>();
    EXPECT_EQ(pred_right->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(pred_right->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto prl = pred_right->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto prr = pred_right->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(prl->GetTupleIdx(), 0);
    EXPECT_EQ(prl->GetValueIdx(), ol_w_id_idx);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(prr->GetValue()), 3);

    // IdxScan OutputSchema
    auto idx_scan_schema = idx_scan->GetOutputSchema();
    EXPECT_EQ(idx_scan_schema->GetColumns().size(), 1);
    auto idx_scan_expr = idx_scan_schema->GetColumn(0).GetExpr();
    EXPECT_EQ(idx_scan_expr->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    auto idx_scan_expr_dve = idx_scan_expr.CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(idx_scan_expr_dve->GetTupleIdx(), 0);
    EXPECT_EQ(idx_scan_expr_dve->GetValueIdx(), ol_amount_idx);
  };

  std::string query =
      "SELECT SUM(OL_AMOUNT) AS OL_TOTAL FROM \"ORDER LINE\" WHERE OL_O_ID=1 AND OL_D_ID=2 AND OL_W_ID=3";
  OptimizeQuery(query, tbl_order_line_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanDeliveryTests, UpdateCustomBalanceDeliveryCount) {
  auto check = [](TpccPlanTest *test, catalog::table_oid_t tbl_oid, std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto &schema = test->accessor_->GetSchema(tbl_oid);
    std::vector<catalog::col_oid_t> update_oids{schema.GetColumn("c_balance").Oid(),
                                                schema.GetColumn("c_delivery_cnt").Oid()};

    std::vector<int> update_idxs(2, -1);
    for (size_t i = 0; i < schema.GetColumns().size(); i++) {
      if (schema.GetColumns()[i].Name() == "c_balance") {
        update_idxs[0] = static_cast<int>(i);
      } else if (schema.GetColumns()[i].Name() == "c_delivery_cnt") {
        update_idxs[1] = static_cast<int>(i);
      }
    }

    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::UPDATE);
    auto update = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    EXPECT_EQ(update->GetDatabaseOid(), test->db_);
    EXPECT_EQ(update->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());
    EXPECT_EQ(update->GetTableOid(), test->tbl_customer_);
    EXPECT_EQ(update->GetUpdatePrimaryKey(), false);

    EXPECT_EQ(update->GetSetClauses().size(), 2);
    for (auto idx = 0; idx < 2; idx++) {
      EXPECT_EQ(update->GetSetClauses()[idx].first, update_oids[idx]);
      auto expr = update->GetSetClauses()[idx].second;
      EXPECT_EQ(expr->GetExpressionType(), parser::ExpressionType::OPERATOR_PLUS);
      auto dve = expr->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
      EXPECT_EQ(dve->GetTupleIdx(), 0);
      EXPECT_EQ(dve->GetValueIdx(), update_idxs[idx]);

      auto cve = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
      EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
    }

    // Idx Scan, full output schema
    EXPECT_EQ(update->GetChildren().size(), 1);
    EXPECT_EQ(update->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);

    // Check Child
    auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(update->GetChild(0));
    EXPECT_EQ(idx_scan->IsForUpdate(), true);
    EXPECT_EQ(idx_scan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(idx_scan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDesc
    // Under the TrivialCostModel, the index picked is the secondary
    auto &idx_desc = idx_scan->GetIndexScanDescription();
    EXPECT_EQ(idx_desc.GetExpressionTypeList().size(), 2);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList().size(), 2);
    EXPECT_EQ(idx_desc.GetValueList().size(), 2);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetExpressionTypeList()[1], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[0], schema.GetColumn("c_w_id").Oid());
    EXPECT_EQ(idx_desc.GetTupleColumnIdList()[1], schema.GetColumn("c_d_id").Oid());
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[0]), 1);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(idx_desc.GetValueList()[1]), 2);
    // EXPECT_EQ(idx_desc.GetExpressionTypeList()[2], parser::ExpressionType::COMPARE_EQUAL);
    // EXPECT_EQ(idx_desc.GetTupleColumnIdList()[2], schema.GetColumn("c_id").Oid());
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
      "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE+1, C_DELIVERY_CNT=C_DELIVERY_CNT+1"
      "WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3";
  OptimizeUpdate(query, tbl_customer_, check);
}

}  // namespace terrier
