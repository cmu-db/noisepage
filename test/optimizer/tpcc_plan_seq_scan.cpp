#include "parser/postgresparser.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/optimizer.h"
#include "util/tpcc/tpcc_plan_test.h"
#include "util/test_harness.h"

namespace terrier::optimizer {

class TpccPlanSeqScanTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanSeqScanTests, SimpleSeqScanSelect) {
  parser::PostgresParser pgparser;
  std::string query = "SELECT NO_O_ID FROM NEW_ORDER";
  auto stmt_list = pgparser.BuildParseTree(query);
  auto sel_stmt = reinterpret_cast<parser::SelectStatement*>(stmt_list[0].get());

  BeginTransaction();

  BindColumnValues(sel_stmt->GetSelectColumn(0).get(), tbl_new_order_, "new_order");

  auto get = new OperatorExpression(LogicalGet::make(db_, accessor_->GetDefaultNamespace(), tbl_new_order_, {}, "new_order", false), {});

  std::vector<common::ManagedPointer<const parser::AbstractExpression>> output;
  output.emplace_back(sel_stmt->GetSelectColumn(0).get());

  auto property_set = new PropertySet();
  auto query_info = QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);

  auto optimizer = new Optimizer(new TrivialCostModel());
  auto plan = optimizer->BuildPlanTree(get, query_info, txn_, settings_manager_, accessor_);

  EXPECT_EQ(plan->GetChildrenSize(), 0);
  EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  auto seq = std::dynamic_pointer_cast<planner::SeqScanPlanNode>(plan);
  EXPECT_EQ(seq->GetScanPredicate(), nullptr);
  EXPECT_EQ(seq->IsForUpdate(), false);
  EXPECT_EQ(seq->IsParallel(), settings_manager_->GetBool(settings::Param::parallel_execution));
  EXPECT_EQ(seq->GetDatabaseOid(), db_);
  EXPECT_EQ(seq->GetNamespaceOid(), accessor_->GetDefaultNamespace());

  auto &schema = accessor_->GetSchema(tbl_new_order_);
  unsigned id_offset = 0;
  for (size_t idx = 0; idx < schema.GetColumns().size(); idx++)
    if (schema.GetColumns()[idx].Name() == "no_o_id")
      id_offset = static_cast<unsigned>(idx);
  EXPECT_EQ(seq->GetTableOid(), tbl_new_order_);

  EXPECT_EQ(seq->GetColumnIds().size(), 1);
  EXPECT_EQ(seq->GetColumnIds()[0], schema.GetColumn("no_o_id").Oid());

  auto output_schema = seq->GetOutputSchema();
  EXPECT_EQ(output_schema->GetColumns().size(), 1);
  EXPECT_EQ(output_schema->GetColumn(0).GetName(), "no_o_id");
  EXPECT_EQ(output_schema->GetColumn(0).GetType(), schema.GetColumn("no_o_id").Type());

  EXPECT_EQ(output_schema->GetDerivedTargets().size(), 0);
  EXPECT_EQ(output_schema->GetDirectMapList().size(), 1);

  auto dml = output_schema->GetDirectMapList()[0];
  EXPECT_EQ(dml.first, 0);
  EXPECT_EQ(dml.second.first, 0);
  EXPECT_EQ(dml.second.second, id_offset);

  delete get;
  delete property_set;
  delete optimizer;

  EndTransaction(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanSeqScanTests, SimpleSeqScanSelectWithPredicate) {
  parser::PostgresParser pgparser;
  std::string query = "SELECT o_id from \"ORDER\" where o_carrier_id = 5";
  auto stmt_list = pgparser.BuildParseTree(query);
  auto sel_stmt = reinterpret_cast<parser::SelectStatement*>(stmt_list[0].get());
  auto predicate = sel_stmt->GetSelectCondition()->Copy();

  BeginTransaction();

  BindColumnValues(predicate, tbl_order_, "order");
  BindColumnValues(sel_stmt->GetSelectColumn(0).get(), tbl_order_, "order");

  auto get = new OperatorExpression(LogicalGet::make(db_, accessor_->GetDefaultNamespace(), tbl_order_, {}, "order", false), {});

  // Build Filter
  auto predicates = ExtractPredicates(predicate);
  auto children = {get};
  auto filter = new OperatorExpression(LogicalFilter::make(std::move(predicates)), std::move(children));

  std::vector<common::ManagedPointer<const parser::AbstractExpression>> output;
  output.emplace_back(sel_stmt->GetSelectColumn(0).get());

  auto property_set = new PropertySet();
  auto query_info = QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);

  auto optimizer = new Optimizer(new TrivialCostModel());
  auto plan = optimizer->BuildPlanTree(filter, query_info, txn_, settings_manager_, accessor_);

  // Find column offset that matches with o_carrier_id
  unsigned offset = 0;
  unsigned o_id_offset = 0;
  auto &schema = accessor_->GetSchema(tbl_order_);
  for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
    if (schema.GetColumns()[idx].Name() == "o_carrier_id")
      offset = static_cast<unsigned>(idx);
    else if (schema.GetColumns()[idx].Name() == "o_id")
      o_id_offset = static_cast<unsigned>(idx);
  }

  EXPECT_EQ(plan->GetChildrenSize(), 0);
  EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  auto seq = std::dynamic_pointer_cast<planner::SeqScanPlanNode>(plan); 
  auto scan_pred = seq->GetScanPredicate();
  EXPECT_TRUE(scan_pred != nullptr);
  EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
  EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);

  auto *dve = reinterpret_cast<const parser::DerivedValueExpression*>(scan_pred->GetChild(0).get());
  auto *cve = reinterpret_cast<const parser::ConstantValueExpression*>(scan_pred->GetChild(1).get());
  EXPECT_EQ(dve->GetTupleIdx(), 0);
  EXPECT_EQ(dve->GetValueIdx(), offset); // ValueIdx() should be offset into underlying tuple
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 5);

  EXPECT_EQ(seq->IsForUpdate(), false);
  EXPECT_EQ(seq->IsParallel(), settings_manager_->GetBool(settings::Param::parallel_execution));
  EXPECT_EQ(seq->GetDatabaseOid(), db_);
  EXPECT_EQ(seq->GetNamespaceOid(), accessor_->GetDefaultNamespace());
  EXPECT_EQ(seq->GetTableOid(), tbl_order_);

  EXPECT_EQ(seq->GetColumnIds().size(), 1);
  EXPECT_EQ(seq->GetColumnIds()[0], schema.GetColumn("o_id").Oid());

  auto output_schema = seq->GetOutputSchema();
  EXPECT_EQ(output_schema->GetColumns().size(), 1);
  EXPECT_EQ(output_schema->GetColumn(0).GetName(), "o_id");
  EXPECT_EQ(output_schema->GetColumn(0).GetType(), schema.GetColumn("o_id").Type());

  EXPECT_EQ(output_schema->GetDerivedTargets().size(), 0);
  EXPECT_EQ(output_schema->GetDirectMapList().size(), 1);

  auto dml = output_schema->GetDirectMapList()[0];
  EXPECT_EQ(dml.first, 0);
  EXPECT_EQ(dml.second.first, 0);
  EXPECT_EQ(dml.second.second, o_id_offset);

  delete filter;
  delete property_set;
  delete optimizer;
  delete predicate;

  EndTransaction(true);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanSeqScanTests, SimpleSeqScanSelectWithPredicateOrderBy) {
  parser::PostgresParser pgparser;
  std::string query = "SELECT o_id from \"ORDER\" where o_carrier_id = 5 ORDER BY o_ol_cnt DESC";
  auto stmt_list = pgparser.BuildParseTree(query);
  auto sel_stmt = reinterpret_cast<parser::SelectStatement*>(stmt_list[0].get());
  auto predicate = sel_stmt->GetSelectCondition()->Copy();

  BeginTransaction();

  BindColumnValues(predicate, tbl_order_, "order");
  BindColumnValues(sel_stmt->GetSelectColumn(0).get(), tbl_order_, "order");
  BindColumnValues(sel_stmt->GetSelectOrderBy()->GetOrderByExpression(0).get(), tbl_order_, "order");

  auto get = new OperatorExpression(LogicalGet::make(db_, accessor_->GetDefaultNamespace(), tbl_order_, {}, "order", false), {});

  // Build Filter
  auto predicates = ExtractPredicates(predicate);
  auto children = {get};
  auto filter = new OperatorExpression(LogicalFilter::make(std::move(predicates)), std::move(children));

  std::vector<common::ManagedPointer<const parser::AbstractExpression>> output;
  output.emplace_back(sel_stmt->GetSelectColumn(0).get());

  // Build SortProperty
  auto order = sel_stmt->GetSelectOrderBy();
  std::vector<planner::OrderByOrderingType> sort_dirs;
  std::vector<common::ManagedPointer<const parser::AbstractExpression>> order_bys;
  EXPECT_EQ(order->GetOrderByExpressionsSize(), 1);
  order_bys.push_back(order->GetOrderByExpression(0));
  sort_dirs.push_back(order->GetOrderByTypes()[0] == parser::OrderType::kOrderAsc ?
                      planner::OrderByOrderingType::ASC : planner::OrderByOrderingType::DESC);
  auto sort_prop = new PropertySort(order_bys, sort_dirs);

  auto property_set = new PropertySet();
  property_set->AddProperty(sort_prop);

  auto query_info = QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);

  auto optimizer = new Optimizer(new TrivialCostModel());
  auto plan = optimizer->BuildPlanTree(filter, query_info, txn_, settings_manager_, accessor_);

  // Expect a Projection <= OrderBy <= SeqScan
  // Hardcoding this is an unfortunate
  auto &schema = accessor_->GetSchema(tbl_order_);

  // Find column offset that matches with o_carrier_id
  unsigned carrier_offset = 0;
  unsigned o_id_offset = 0;
  unsigned o_ol_cnt_offset = 0;
  for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
    if (schema.GetColumns()[idx].Name() == "o_carrier_id")
      carrier_offset = static_cast<unsigned>(idx);
    else if (schema.GetColumns()[idx].Name() == "o_id")
      o_id_offset = static_cast<unsigned>(idx);
    else if (schema.GetColumns()[idx].Name() == "o_ol_cnt")
      o_ol_cnt_offset = static_cast<unsigned>(idx);
  }

  EXPECT_EQ(plan->GetChildrenSize(), 1);
  EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(plan->GetOutputSchema()->GetColumns().size(), 1);
  EXPECT_EQ(plan->GetOutputSchema()->GetColumn(0).GetName(), "o_id");
  EXPECT_EQ(plan->GetOutputSchema()->GetColumn(0).GetType(), schema.GetColumn("o_id").Type());
  EXPECT_EQ(plan->GetOutputSchema()->GetDerivedTargets().size(), 0);
  EXPECT_EQ(plan->GetOutputSchema()->GetDirectMapList().size(), 1);
  EXPECT_EQ(plan->GetOutputSchema()->GetDirectMapList()[0].first, 0);
  EXPECT_EQ(plan->GetOutputSchema()->GetDirectMapList()[0].second.first, 0);
  EXPECT_EQ(plan->GetOutputSchema()->GetDirectMapList()[0].second.second, 1);

  // Order By
  auto planc = plan->GetChild(0);
  EXPECT_EQ(planc->GetChildrenSize(), 1);
  EXPECT_EQ(planc->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  auto orderby = reinterpret_cast<const planner::OrderByPlanNode*>(planc);
  EXPECT_EQ(orderby->HasLimit(), false);
  EXPECT_EQ(orderby->GetSortKeys().size(), 1);
  EXPECT_EQ(orderby->GetSortKeys()[0].second, planner::OrderByOrderingType::DESC);
  auto sortkey = reinterpret_cast<const parser::DerivedValueExpression*>(orderby->GetSortKeys()[0].first);
  EXPECT_TRUE(sortkey != nullptr);
  EXPECT_EQ(sortkey->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(sortkey->GetTupleIdx(), 0);
  EXPECT_EQ(sortkey->GetValueIdx(), 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetColumns().size(), 2);
  EXPECT_EQ(planc->GetOutputSchema()->GetColumn(0).GetName(), "o_ol_cnt");
  EXPECT_EQ(planc->GetOutputSchema()->GetColumn(0).GetType(), schema.GetColumn("o_ol_cnt").Type());
  EXPECT_EQ(planc->GetOutputSchema()->GetColumn(1).GetName(), "o_id");
  EXPECT_EQ(planc->GetOutputSchema()->GetColumn(1).GetType(), schema.GetColumn("o_id").Type());
  EXPECT_EQ(planc->GetOutputSchema()->GetDerivedTargets().size(), 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList().size(), 2);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[0].first, 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[0].second.first, 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[0].second.second, 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[1].first, 1);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[1].second.first, 0);
  EXPECT_EQ(planc->GetOutputSchema()->GetDirectMapList()[1].second.second, 1);

  // Seq Scan
  auto plans = planc->GetChild(0);
  EXPECT_EQ(plans->GetChildrenSize(), 0);
  EXPECT_EQ(plans->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);
  auto seq = reinterpret_cast<const planner::SeqScanPlanNode*>(plans);

  // Check scan predicate
  auto scan_pred = seq->GetScanPredicate();
  EXPECT_TRUE(scan_pred != nullptr);
  EXPECT_EQ(scan_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(scan_pred->GetChildrenSize(), 2);
  EXPECT_EQ(scan_pred->GetChild(0)->GetExpressionType(), parser::ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(scan_pred->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  auto *dve = reinterpret_cast<const parser::DerivedValueExpression*>(scan_pred->GetChild(0).get());
  auto *cve = reinterpret_cast<const parser::ConstantValueExpression*>(scan_pred->GetChild(1).get());
  EXPECT_EQ(dve->GetTupleIdx(), 0);
  EXPECT_EQ(dve->GetValueIdx(), carrier_offset); // ValueIdx() should be offset into underlying tuple (all cols)
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(cve->GetValue()), 5);

  // Check SeqScan
  EXPECT_EQ(seq->IsForUpdate(), false);
  EXPECT_EQ(seq->IsParallel(), settings_manager_->GetBool(settings::Param::parallel_execution));
  EXPECT_EQ(seq->GetDatabaseOid(), db_);
  EXPECT_EQ(seq->GetNamespaceOid(), accessor_->GetDefaultNamespace());
  EXPECT_EQ(seq->GetTableOid(), tbl_order_);
  EXPECT_EQ(seq->GetColumnIds().size(), 2);
  EXPECT_EQ(seq->GetColumnIds()[0], schema.GetColumn("o_ol_cnt").Oid());
  EXPECT_EQ(seq->GetColumnIds()[1], schema.GetColumn("o_id").Oid());

  // Check SeqScan OutputSchema
  auto output_schema = seq->GetOutputSchema();
  EXPECT_EQ(output_schema->GetColumns().size(), 2);
  EXPECT_EQ(output_schema->GetColumn(0).GetName(), "o_ol_cnt");
  EXPECT_EQ(output_schema->GetColumn(0).GetType(), schema.GetColumn("o_ol_cnt").Type());
  EXPECT_EQ(output_schema->GetColumn(1).GetName(), "o_id");
  EXPECT_EQ(output_schema->GetColumn(1).GetType(), schema.GetColumn("o_id").Type());
  EXPECT_EQ(output_schema->GetDerivedTargets().size(), 0);
  EXPECT_EQ(output_schema->GetDirectMapList().size(), 2);
  EXPECT_EQ(output_schema->GetDirectMapList()[0].first, 0);
  EXPECT_EQ(output_schema->GetDirectMapList()[0].second.first, 0);
  EXPECT_EQ(output_schema->GetDirectMapList()[0].second.second, o_ol_cnt_offset);
  EXPECT_EQ(output_schema->GetDirectMapList()[1].first, 1);
  EXPECT_EQ(output_schema->GetDirectMapList()[1].second.first, 0);
  EXPECT_EQ(output_schema->GetDirectMapList()[1].second.second, o_id_offset);

  delete filter;
  delete property_set;
  delete optimizer;
  delete predicate;

  EndTransaction(true);
}

}  // namespace terrier::optimizer
