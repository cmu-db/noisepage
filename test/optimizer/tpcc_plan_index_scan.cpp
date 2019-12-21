#include <memory>
#include <string>

#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression_util.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier::optimizer {

class TpccPlanIndexScanTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, SimplePredicateIndexScan) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Get Schema
    auto &schema = test->accessor_->GetSchema(test->tbl_new_order_);
    unsigned no_d_id_offset = 0;
    for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
      auto idx_u = static_cast<unsigned>(idx);
      if (schema.GetColumn(idx_u).Name() == "no_d_id") no_d_id_offset = static_cast<unsigned>(idx_u);
    }

    // Should use New Order Primary Key (NO_W_ID, NO_D_ID, NO_O_ID)
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(plan->GetChildrenSize(), 0);
    auto index_plan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    EXPECT_EQ(index_plan->GetIndexOid(), test->pk_new_order_);
    EXPECT_EQ(index_plan->GetColumnIds().size(), 1);
    EXPECT_EQ(index_plan->GetColumnIds()[0], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(index_plan->IsForUpdate(), false);
    EXPECT_EQ(index_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(index_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    auto &index_desc = index_plan->GetIndexScanDescription();
    EXPECT_EQ(index_desc.GetTupleColumnIdList().size(), 1);
    EXPECT_EQ(index_desc.GetExpressionTypeList().size(), 1);
    EXPECT_EQ(index_desc.GetValueList().size(), 1);
    EXPECT_EQ(index_desc.GetTupleColumnIdList()[0], schema.GetColumn("no_d_id").Oid());
    EXPECT_EQ(index_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(index_desc.GetValueList()[0].Type(), type::TypeId::INTEGER);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_desc.GetValueList()[0]), 1);

    auto scan_pred = index_plan->GetScanPredicate();
    EXPECT_TRUE(scan_pred != nullptr);
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto dve = scan_pred->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto cve = scan_pred->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), no_d_id_offset);  // ValueIdx() should be offset into underlying tuple
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1";
  OptimizeQuery(query, tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, SimplePredicateFlippedIndexScan) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Get Schema
    auto &schema = test->accessor_->GetSchema(test->tbl_new_order_);
    unsigned no_d_id_offset = 0;
    for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
      auto idx_u = static_cast<unsigned>(idx);
      if (schema.GetColumn(idx_u).Name() == "no_d_id") no_d_id_offset = static_cast<unsigned>(idx_u);
    }

    // Should use New Order Primary Key (NO_W_ID, NO_D_ID, NO_O_ID)
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(plan->GetChildrenSize(), 0);
    auto index_plan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    EXPECT_EQ(index_plan->GetIndexOid(), test->pk_new_order_);
    EXPECT_EQ(index_plan->GetColumnIds().size(), 1);
    EXPECT_EQ(index_plan->GetColumnIds()[0], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(index_plan->IsForUpdate(), false);
    EXPECT_EQ(index_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(index_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    auto &index_desc = index_plan->GetIndexScanDescription();
    EXPECT_EQ(index_desc.GetTupleColumnIdList().size(), 1);
    EXPECT_EQ(index_desc.GetExpressionTypeList().size(), 1);
    EXPECT_EQ(index_desc.GetValueList().size(), 1);
    EXPECT_EQ(index_desc.GetTupleColumnIdList()[0], schema.GetColumn("no_d_id").Oid());
    EXPECT_EQ(index_desc.GetExpressionTypeList()[0],
              parser::ExpressionUtil::ReverseComparisonExpressionType(parser::ExpressionType::COMPARE_LESS_THAN));
    EXPECT_EQ(index_desc.GetValueList()[0].Type(), type::TypeId::INTEGER);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_desc.GetValueList()[0]), 1);

    auto scan_pred = index_plan->GetScanPredicate();
    EXPECT_TRUE(scan_pred != nullptr);
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_LESS_THAN);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto dve = scan_pred->GetChild(1).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto cve = scan_pred->GetChild(0).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), no_d_id_offset);  // ValueIdx() should be offset into underlying tuple
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE 1 < NO_D_ID";
  OptimizeQuery(query, tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, IndexFulfillSort) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Get Schema
    auto &schema = test->accessor_->GetSchema(test->tbl_new_order_);

    // Should use New Order Primary Key (NO_W_ID, NO_D_ID, NO_O_ID)
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(plan->GetChildrenSize(), 0);
    auto index_plan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    EXPECT_EQ(index_plan->GetIndexOid(), test->pk_new_order_);
    EXPECT_EQ(index_plan->GetColumnIds().size(), 1);
    EXPECT_EQ(index_plan->GetColumnIds()[0], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(index_plan->IsForUpdate(), false);
    EXPECT_EQ(index_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(index_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    EXPECT_EQ(index_plan->GetIndexScanDescription().GetTupleColumnIdList().size(), 0);
    EXPECT_EQ(index_plan->GetIndexScanDescription().GetExpressionTypeList().size(), 0);
    EXPECT_EQ(index_plan->GetIndexScanDescription().GetValueList().size(), 0);
    EXPECT_EQ(index_plan->GetScanPredicate().Get(), nullptr);
  };

  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID";
  OptimizeQuery(query, tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, IndexCannotFulfillSort) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // If we need to use a SeqScan to fulfill sort, then there must be a child
    // Since the root is an OrderBy
    EXPECT_NE(plan->GetChildrenSize(), 0);
    EXPECT_NE(plan->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  };

  OptimizeQuery("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID DESC", tbl_new_order_, check);
  OptimizeQuery("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_D_ID", tbl_new_order_, check);
  OptimizeQuery("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID, NO_D_ID DESC", tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, IndexFulfillSortAndPredicate) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Get Schema
    auto &schema = test->accessor_->GetSchema(test->tbl_new_order_);
    unsigned no_w_id_offset = 0;
    for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
      auto idx_u = static_cast<unsigned>(idx);
      if (schema.GetColumn(idx_u).Name() == "no_w_id") {
        no_w_id_offset = idx_u;
      }
    }

    // Should use New Order Primary Key (NO_W_ID, NO_D_ID, NO_O_ID)
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(plan->GetChildrenSize(), 0);
    auto index_plan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    EXPECT_EQ(index_plan->GetIndexOid(), test->pk_new_order_);
    EXPECT_EQ(index_plan->GetColumnIds().size(), 1);
    EXPECT_EQ(index_plan->GetColumnIds()[0], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(index_plan->IsForUpdate(), false);
    EXPECT_EQ(index_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(index_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDescription
    auto &index_desc = index_plan->GetIndexScanDescription();
    EXPECT_EQ(index_desc.GetTupleColumnIdList().size(), 1);
    EXPECT_EQ(index_desc.GetExpressionTypeList().size(), 1);
    EXPECT_EQ(index_desc.GetValueList().size(), 1);
    EXPECT_EQ(index_desc.GetTupleColumnIdList()[0], schema.GetColumn("no_w_id").Oid());
    EXPECT_EQ(index_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(index_desc.GetValueList()[0].Type(), type::TypeId::INTEGER);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_desc.GetValueList()[0]), 1);

    // Check Index Scan Predicate
    auto scan_pred = index_plan->GetScanPredicate();
    EXPECT_TRUE(scan_pred != nullptr);
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto dve = scan_pred->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto cve = scan_pred->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), no_w_id_offset);  // ValueIdx() should be offset into underlying tuple
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_W_ID = 1 ORDER BY NO_W_ID";
  OptimizeQuery(query, tbl_new_order_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanIndexScanTests, IndexFulfillSortAndPredicateWithLimitOffset) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Get Schema
    auto &schema = test->accessor_->GetSchema(test->tbl_new_order_);
    unsigned no_w_id_offset = 0;
    for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
      auto idx_u = static_cast<unsigned>(idx);
      if (schema.GetColumn(idx_u).Name() == "no_w_id") {
        no_w_id_offset = idx_u;
      }
    }
    EXPECT_EQ(plan->GetChildrenSize(), 1);
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);

    // Limit
    auto planl = plan->GetChild(0);
    EXPECT_EQ(planl->GetChildrenSize(), 1);
    EXPECT_EQ(planl->GetPlanNodeType(), planner::PlanNodeType::LIMIT);
    auto limit_plan = reinterpret_cast<const planner::LimitPlanNode *>(planl);
    EXPECT_EQ(limit_plan->GetLimit(), sel_stmt->GetSelectLimit()->GetLimit());
    EXPECT_EQ(limit_plan->GetOffset(), sel_stmt->GetSelectLimit()->GetOffset());

    // Order By
    auto planc = planl->GetChild(0);
    EXPECT_EQ(planc->GetChildrenSize(), 1);
    EXPECT_EQ(planc->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
    auto orderby = reinterpret_cast<const planner::OrderByPlanNode *>(planc);
    EXPECT_EQ(orderby->HasLimit(), true);
    EXPECT_EQ(orderby->GetLimit(), sel_stmt->GetSelectLimit()->GetLimit());
    EXPECT_EQ(orderby->GetOffset(), sel_stmt->GetSelectLimit()->GetOffset());
    EXPECT_EQ(orderby->GetSortKeys().size(), 1);
    EXPECT_EQ(orderby->GetSortKeys()[0].second, optimizer::OrderByOrderingType::ASC);
    auto sortkey = orderby->GetSortKeys()[0].first.CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_TRUE(sortkey != nullptr);
    EXPECT_EQ(sortkey->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(sortkey->GetTupleIdx(), 0);
    EXPECT_EQ(sortkey->GetValueIdx(), 0);

    // Should use New Order Primary Key (NO_W_ID, NO_D_ID, NO_O_ID)
    auto plani = planc->GetChild(0);
    EXPECT_EQ(plani->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(plani->GetChildrenSize(), 0);
    auto index_plan = reinterpret_cast<const planner::IndexScanPlanNode *>(plani);
    EXPECT_EQ(index_plan->GetIndexOid(), test->pk_new_order_);
    EXPECT_EQ(index_plan->GetColumnIds().size(), 2);
    EXPECT_EQ(index_plan->GetColumnIds()[0], schema.GetColumn("no_w_id").Oid());
    EXPECT_EQ(index_plan->GetColumnIds()[1], schema.GetColumn("no_o_id").Oid());
    EXPECT_EQ(index_plan->IsForUpdate(), false);
    EXPECT_EQ(index_plan->GetDatabaseOid(), test->db_);
    EXPECT_EQ(index_plan->GetNamespaceOid(), test->accessor_->GetDefaultNamespace());

    // Check IndexScanDescription
    auto &index_desc = index_plan->GetIndexScanDescription();
    EXPECT_EQ(index_desc.GetTupleColumnIdList().size(), 1);
    EXPECT_EQ(index_desc.GetExpressionTypeList().size(), 1);
    EXPECT_EQ(index_desc.GetValueList().size(), 1);
    EXPECT_EQ(index_desc.GetTupleColumnIdList()[0], schema.GetColumn("no_w_id").Oid());
    EXPECT_EQ(index_desc.GetExpressionTypeList()[0], parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(index_desc.GetValueList()[0].Type(), type::TypeId::INTEGER);
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(index_desc.GetValueList()[0]), 1);

    // Check Index Scan Predicate
    auto scan_pred = index_plan->GetScanPredicate();
    EXPECT_TRUE(scan_pred != nullptr);
    EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
    EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
    EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
    auto dve = scan_pred->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto cve = scan_pred->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
    EXPECT_EQ(dve->GetTupleIdx(), 0);
    EXPECT_EQ(dve->GetValueIdx(), no_w_id_offset);  // ValueIdx() should be offset into underlying tuple
    EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 1);
  };

  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_W_ID = 1 ORDER BY NO_W_ID LIMIT 15 OFFSET 445";
  OptimizeQuery(query, tbl_new_order_, check);
}

}  // namespace terrier::optimizer
