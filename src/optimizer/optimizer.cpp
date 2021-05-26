#include "optimizer/optimizer.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/scoped_timer.h"
#include "optimizer/binding.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimization_context.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/plan_generator.h"
#include "optimizer/properties.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/rule.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace noisepage::optimizer {

void Optimizer::Reset() { context_ = std::make_unique<OptimizerContext>(common::ManagedPointer(cost_model_)); }

std::unique_ptr<OptimizeResult> Optimizer::BuildPlanTree(
    transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, StatsStorage *storage,
    QueryInfo query_info, std::unique_ptr<AbstractOptimizerNode> op_tree,
    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters) {
  context_->SetTxn(txn);
  context_->SetCatalogAccessor(accessor);
  context_->SetStatsStorage(storage);
  context_->SetParams(parameters);
  auto optimize_result = std::make_unique<OptimizeResult>();

  // Generate initial operator tree from query tree
  GroupExpression *gexpr = nullptr;
  UNUSED_ATTRIBUTE bool insert = context_->RecordOptimizerNodeIntoGroup(common::ManagedPointer(op_tree), &gexpr);
  NOISEPAGE_ASSERT(insert && gexpr, "Logical expression tree should insert");

  group_id_t root_id = gexpr->GetGroupID();

  // Physical properties
  PropertySet *phys_properties = query_info.GetPhysicalProperties();

  // Give ManagedPointers to ChooseBestPlan
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_exprs;
  for (auto expr : query_info.GetOutputExprs()) {
    output_exprs.push_back(expr);
  }

  try {
    OptimizeLoop(root_id, phys_properties);
  } catch (OptimizerException &e) {
    OPTIMIZER_LOG_WARN("Optimize Loop ended prematurely: {0}", e.what());
  }

  try {
    PlanGenerator generator(optimize_result->GetPlanMetaData());
    auto best_plan = ChooseBestPlan(txn, accessor, root_id, phys_properties, output_exprs, &generator);
    optimize_result->SetPlanNode(std::move(best_plan));
    // Reset memo after finishing the optimization
    Reset();
    return optimize_result;
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

std::unique_ptr<planner::AbstractPlanNode> Optimizer::ChooseBestPlan(
    transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, group_id_t id,
    PropertySet *required_props, const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
    PlanGenerator *generator) {
  Group *group = context_->GetMemo().GetGroupByID(id);
  auto gexpr = group->GetBestExpression(required_props);

  OPTIMIZER_LOG_TRACE("Choosing best plan for group " + std::to_string(gexpr->GetGroupID().UnderlyingValue()) +
                      " with op " + gexpr->Contents()->GetName());

  std::vector<group_id_t> child_groups = gexpr->GetChildGroupIDs();

  // required_input_props is owned by the GroupExpression
  auto required_input_props = gexpr->GetInputProperties(required_props);
  NOISEPAGE_ASSERT(required_input_props.size() == child_groups.size(), "input properties and group size mismatch");

  // Firstly derive input/output columns
  InputColumnDeriver deriver(txn, accessor);
  auto output_input_cols_pair = deriver.DeriveInputColumns(gexpr, required_props, required_cols, &context_->GetMemo());

  auto &output_cols = output_input_cols_pair.first;
  auto &input_cols = output_input_cols_pair.second;
  NOISEPAGE_ASSERT(input_cols.size() == required_input_props.size(),
                   "input columns and input properties size mismatch");

  // Derive chidren plans first because they are useful in the derivation of
  // root plan. Also keep propagate expression to column offset mapping
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans;
  std::vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    for (unsigned offset = 0; offset < input_cols[i].size(); ++offset) {
      NOISEPAGE_ASSERT(input_cols[i][offset] != nullptr, "invalid input column found");
      child_expr_map[input_cols[i][offset]] = offset;
    }

    auto child_plan = ChooseBestPlan(txn, accessor, child_groups[i], required_input_props[i], input_cols[i], generator);
    NOISEPAGE_ASSERT(child_plan != nullptr, "child should have derived a non-null plan...");
    children_plans.emplace_back(std::move(child_plan));
    children_expr_map.push_back(child_expr_map);
  }

  // Derive root plan
  auto *op = new OperatorNode(gexpr->Contents(), {}, txn);

  planner::PlanMetaData::PlanNodeMetaData plan_node_meta_data(context_->GetMemo().GetGroupByID(id)->GetNumRows());
  auto plan = generator->ConvertOpNode(txn, accessor, op, required_props, required_cols, output_cols,
                                       std::move(children_plans), std::move(children_expr_map), plan_node_meta_data);
  OPTIMIZER_LOG_TRACE("Finish Choosing best plan for group " + std::to_string(id.UnderlyingValue()));

  delete op;
  return plan;
}

void Optimizer::OptimizeLoop(group_id_t root_group_id, PropertySet *required_props) {
  auto root_context = new OptimizationContext(context_.get(), required_props->Copy());
  auto task_stack = new OptimizerTaskStack();
  context_->SetTaskPool(task_stack);
  context_->AddOptimizationContext(root_context);

  // Perform rewrite first
  task_stack->Push(new TopDownRewrite(root_group_id, root_context, RuleSetName::PREDICATE_PUSH_DOWN));
  task_stack->Push(new BottomUpRewrite(root_group_id, root_context, RuleSetName::UNNEST_SUBQUERY, false));
  ExecuteTaskStack(task_stack, root_group_id, root_context);

  // Perform optimization after the rewrite
  Memo &memo = context_->GetMemo();
  task_stack->Push(new OptimizeGroup(memo.GetGroupByID(root_group_id), root_context));

  // Derive stats for the only one logical expression before optimizing
  task_stack->Push(new DeriveStats(memo.GetGroupByID(root_group_id)->GetLogicalExpression(), root_context));

  ExecuteTaskStack(task_stack, root_group_id, root_context);
}

void Optimizer::ExecuteTaskStack(OptimizerTaskStack *task_stack, group_id_t root_group_id,
                                 OptimizationContext *root_context) {
  auto root_group = context_->GetMemo().GetGroupByID(root_group_id);
  const auto &required_props = root_context->GetRequiredProperties();

  uint64_t elapsed_time = 0;

  // Iterate through the task stack
  while (!task_stack->Empty()) {
    // Check to see if we have at least one plan, and if we have exceeded our
    // timeout limit
    if (elapsed_time >= task_execution_timeout_ && root_group->HasExpressions(required_props)) {
      throw OPTIMIZER_EXCEPTION("Optimizer task execution timed out");
    }

    uint64_t task_runtime = 0;
    auto task = task_stack->Pop();
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&task_runtime);
      task->Execute();
    }
    delete task;
    elapsed_time += task_runtime;
  }
}

}  // namespace noisepage::optimizer
