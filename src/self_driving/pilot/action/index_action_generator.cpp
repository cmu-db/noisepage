#include "self_driving/pilot/action/index_action_generator.h"

#include <memory>

#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "self_driving/pilot/action/create_index_action.h"
#include "self_driving/pilot/action/drop_index_action.h"
#include "self_driving/pilot/action/index_action_util.h"
#include "self_driving/pilot/action/index_column.h"

namespace noisepage::selfdriving::pilot {

void IndexActionGenerator::GenerateIndexActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                                                std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                                std::vector<action_id_t> *candidate_actions) {
  // Find the "missing" index for each plan and generate the corresponding actions
  for (auto &plan : plans) {
    // Currently using a heuristic to find the scan predicates that are not fully covered by an existing index, and
    // generate actions to build indexes that cover the full predicates
    FindMissingIndex(plan.get(), action_map, candidate_actions);
  }
}

void IndexActionGenerator::FindMissingIndex(const planner::AbstractPlanNode *plan,
                                            std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                            std::vector<action_id_t> *candidate_actions) {
  // Visit all the child nodes
  auto children = plan->GetChildren();
  for (auto child : children) FindMissingIndex(child.Get(), action_map, candidate_actions);

  auto plan_type = plan->GetPlanNodeType();
  if (plan_type == planner::PlanNodeType::SEQSCAN || plan_type == planner::PlanNodeType::INDEXSCAN) {
    auto scan_plan = reinterpret_cast<const planner::AbstractScanPlanNode *>(plan);
    auto predicate = scan_plan->GetScanPredicate();
    // No predicate
    if (predicate == nullptr) return;

    // The index already covers all the indexable columns
    if (plan_type == planner::PlanNodeType::INDEXSCAN &&
        reinterpret_cast<const planner::IndexScanPlanNode *>(plan)->GetCoverAllColumns())
      return;

    // Get table oid
    catalog::table_oid_t table_oid;
    if (plan_type == planner::PlanNodeType::INDEXSCAN)
      table_oid = reinterpret_cast<const planner::IndexScanPlanNode *>(plan)->GetTableOid();
    else
      table_oid = reinterpret_cast<const planner::SeqScanPlanNode *>(plan)->GetTableOid();

    // Generate the "missing" index based on the predicate
    std::vector<common::ManagedPointer<parser::ColumnValueExpression>> equality_columns;
    std::vector<common::ManagedPointer<parser::ColumnValueExpression>> inequality_columns;
    bool indexable = GenerateIndexableColumns(table_oid, predicate, &equality_columns, &inequality_columns);

    // Generate the new index action
    if (indexable && (!equality_columns.empty() || !inequality_columns.empty())) {
      std::string table_name;
      if (!equality_columns.empty())
        table_name = (*equality_columns.begin())->GetTableName();
      else
        table_name = (*inequality_columns.begin())->GetTableName();

      std::vector<IndexColumn> index_columns;
      // For now, just put the inequality predicates at the end of the equality predicates
      for (auto &it : equality_columns) index_columns.emplace_back(it->GetColumnName());

      // TODO(Lin): Don't insert potentially duplicated actions
      // Generate the create index action
      std::string new_index_name = IndexActionUtil::GenerateIndexName(table_name, index_columns);
      auto create_index_action = std::make_unique<CreateIndexAction>(new_index_name, table_name, index_columns);
      action_id_t create_index_action_id = create_index_action->GetActionID();
      action_map->emplace(create_index_action_id, std::move(create_index_action));
      // Only the create index action is valid
      candidate_actions->emplace_back(create_index_action_id);

      // Generate the drop index action
      auto drop_index_action = std::make_unique<DropIndexAction>(new_index_name, table_name, index_columns);
      action_id_t drop_index_action_id = drop_index_action->GetActionID();
      action_map->emplace(drop_index_action_id, std::move(drop_index_action));

      // Populate the reverse actions
      action_map->at(create_index_action_id)->AddReverseAction(drop_index_action_id);
      action_map->at(drop_index_action_id)->AddReverseAction(create_index_action_id);
    }
  }
}

bool IndexActionGenerator::GenerateIndexableColumns(
    catalog::table_oid_t table_oid, common::ManagedPointer<parser::AbstractExpression> expr,
    std::vector<common::ManagedPointer<parser::ColumnValueExpression>> *equality_columns,
    std::vector<common::ManagedPointer<parser::ColumnValueExpression>> *inequality_columns) {
  NOISEPAGE_ASSERT(expr != nullptr, "Unexpected nullptr expression.");

  if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
    // Traverse down the expression tree along conjunction
    bool indexable = true;
    for (const auto &child : expr->GetChildren()) {
      // All the children must be indexable
      indexable = indexable && GenerateIndexableColumns(table_oid, common::ManagedPointer(child), equality_columns,
                                                        inequality_columns);
    }
    return indexable;
  }

  if (expr->HasSubquery()) return false;

  auto type = expr->GetExpressionType();
  switch (type) {
    case parser::ExpressionType::COMPARE_EQUAL:
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
    case parser::ExpressionType::COMPARE_LESS_THAN:
    case parser::ExpressionType::COMPARE_GREATER_THAN:
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
      // Currently supports [column] (=/!=/>/>=/</<=) [value/parameter]
      auto ltype = expr->GetChild(0)->GetExpressionType();
      auto rtype = expr->GetChild(1)->GetExpressionType();

      common::ManagedPointer<parser::ColumnValueExpression> tv_expr;
      if (ltype == parser::ExpressionType::COLUMN_VALUE &&
          (rtype == parser::ExpressionType::VALUE_CONSTANT || rtype == parser::ExpressionType::VALUE_PARAMETER)) {
        tv_expr = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      } else if (rtype == parser::ExpressionType::COLUMN_VALUE && (ltype == parser::ExpressionType::VALUE_CONSTANT ||
                                                                   ltype == parser::ExpressionType::VALUE_PARAMETER)) {
        tv_expr = expr->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
      } else if (ltype == parser::ExpressionType::COLUMN_VALUE && rtype == parser::ExpressionType::COLUMN_VALUE) {
        auto lexpr = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
        auto rexpr = expr->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
        if (lexpr->GetTableOid() == table_oid)
          tv_expr = lexpr;
        else
          tv_expr = rexpr;
      }

      if (type == parser::ExpressionType::COMPARE_EQUAL)
        equality_columns->emplace_back(tv_expr);
      else
        inequality_columns->emplace_back(tv_expr);
      break;
    }
    default:
      // If a predicate can enlarge the result set, then (for now), reject.
      return false;
  }

  return true;
}

}  // namespace noisepage::selfdriving::pilot
