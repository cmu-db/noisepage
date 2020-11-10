#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/postgres/pg_namespace.h"
#include "common/managed_pointer.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/logical_operators.h"
#include "optimizer/optimization_context.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/query_to_operator_transformer.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace noisepage::planner {

class PlanNodeCardinalityTest : public TpccPlanTest {
 public:
  static void CheckCardinality(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                               std::unique_ptr<planner::AbstractPlanNode> plan) {
    const planner::AbstractPlanNode *node = plan.get();
    EXPECT_EQ(node->GetCardinality(), -1);
  }

  void OptimizeQuery(const std::string &query, catalog::table_oid_t tbl_oid,
                     void (*Check)(TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                                   std::unique_ptr<planner::AbstractPlanNode> plan)) {
    BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);
    auto sel_stmt = stmt_list->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
    auto plan = Optimize(query, tbl_oid, parser::StatementType::SELECT);
    Check(this, sel_stmt.Get(), tbl_oid, std::move(plan));
    EndTransaction(true);
  }

  std::unique_ptr<planner::AbstractPlanNode> Optimize(const std::string &query, catalog::table_oid_t tbl_oid,
                                                      parser::StatementType stmt_type) {
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    // Bind + Transform
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn_), db_, DISABLED);
    auto *binder = new binder::BindNodeVisitor(common::ManagedPointer(accessor), db_);
    binder->BindNameToNode(common::ManagedPointer(stmt_list.get()), nullptr, nullptr);
    auto *transformer = new optimizer::QueryToOperatorTransformer(common::ManagedPointer(accessor), db_);
    auto plan = transformer->ConvertToOpExpression(stmt_list->GetStatement(0), common::ManagedPointer(stmt_list.get()));
    delete binder;
    delete transformer;

    auto optimizer = new optimizer::Optimizer(std::make_unique<optimizer::TrivialCostModel>(), task_execution_timeout_);
    std::unique_ptr<planner::AbstractPlanNode> out_plan;
    if (stmt_type == parser::StatementType::SELECT) {
      auto sel_stmt = stmt_list->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

      // Output
      auto output = sel_stmt->GetSelectColumns();

      // Property Sort
      auto property_set = new optimizer::PropertySet();
      std::vector<optimizer::OrderByOrderingType> sort_dirs;
      std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;
      if (sel_stmt->GetSelectOrderBy()) {
        auto order_by = sel_stmt->GetSelectOrderBy();
        auto types = order_by->GetOrderByTypes();
        auto exprs = order_by->GetOrderByExpressions();
        for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
          sort_exprs.emplace_back(exprs[idx]);
          sort_dirs.push_back(types[idx] == parser::OrderType::kOrderAsc ? optimizer::OrderByOrderingType::ASC
                                                                         : optimizer::OrderByOrderingType::DESC);
        }

        auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
        property_set->AddProperty(sort_prop);
      }

      auto query_info = optimizer::QueryInfo(parser::StatementType::SELECT, std::move(output), property_set);
      out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
      delete property_set;
    } else if (stmt_type == parser::StatementType::INSERT) {
      auto ins_stmt = stmt_list->GetStatement(0).CastManagedPointerTo<parser::InsertStatement>();

      auto &schema = accessor_->GetSchema(tbl_oid);
      std::vector<catalog::col_oid_t> col_oids;
      for (auto &col : *ins_stmt->GetInsertColumns()) {
        col_oids.push_back(schema.GetColumn(col).Oid());
      }

      auto property_set = new optimizer::PropertySet();
      auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
      out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
      delete property_set;
    } else {
      auto property_set = new optimizer::PropertySet();
      auto query_info = optimizer::QueryInfo(stmt_type, {}, property_set);
      out_plan = optimizer->BuildPlanTree(txn_, accessor_, stats_storage_.Get(), query_info, std::move(plan));
      delete property_set;
    }

    delete optimizer;
    return out_plan;
  }
};

TEST_F(PlanNodeCardinalityTest, Test) {
  std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\"";
  // std::string query = "SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1 AND NO_W_ID = 2 ORDER BY NO_O_ID LIMIT 1";
  OptimizeQuery(query, tbl_new_order_, CheckCardinality);
}
}  // namespace noisepage::planner