#include <memory>

#include "common/scoped_timer.h"
#include "common/exception.h"

#include "optimizer/optimizer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/binding.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimize_context.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/plan_generator.h"
#include "optimizer/properties.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"

namespace terrier {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer(const CostModels cost_model) : metadata_(nullptr) {

  switch (cost_model) {
    case CostModels::TRIVIAL: {
      // OptimizerMetadata "owns" cost model
      metadata_ = OptimizerMetadata(new TrivialCostModel());
      break;
    }
    default:
      throw OPTIMIZER_EXCEPTION("Invalid cost model");
  }
}

void Optimizer::Reset() {
  // cleanup any existing resources;
  metadata_.SetTaskPool(nullptr);
  metadata_ = OptimizerMetadata(metadata_.cost_model);
}

planner::AbstractPlanNode* Optimizer::BuildPlanTree(
    OperatorExpression* op_tree,
    QueryInfo query_info,
    transaction::TransactionContext *txn,
    settings::SettingsManager *settings) {

  metadata_.txn = txn;

  // Generate initial operator tree from query tree
  GroupExpression *gexpr;
  bool insert = metadata_.RecordTransformedExpression(op_tree, gexpr);
  TERRIER_ASSERT(insert && gexpr, "Logical expression tree should insert");

  GroupID root_id = gexpr->GetGroupID();

  // Give raw pointers to ChooseBestPlan
  std::vector<const parser::AbstractExpression*> output_exprs;
  for (auto expr : query_info.output_exprs) {
    output_exprs.push_back(expr.get());
  }

  try {
    OptimizeLoop(root_id, query_info.physical_props, settings);
  } catch (OptimizerException &e) {
    OPTIMIZER_LOG_WARN("Optimize Loop ended prematurely: %s", e.what());
  }

  try {
    auto best_plan = ChooseBestPlan(root_id, query_info.physical_props, output_exprs);

    // Reset memo after finishing the optimization
    Reset();
    return std::move(best_plan);
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

planner::AbstractPlanNode* Optimizer::ChooseBestPlan(
    GroupID id,
    PropertySet* required_props,
    std::vector<const parser::AbstractExpression *> required_cols) {

  Group *group = metadata_.memo.GetGroupByID(id);
  auto gexpr = group->GetBestExpression(required_props);

  OPTIMIZER_LOG_TRACE("Choosing best plan for group %d with op %s",
            gexpr->GetGroupID(), gexpr->Op().GetName().c_str());

  std::vector<GroupID> child_groups = gexpr->GetChildGroupIDs();

  // required_input_props is owned by the GroupExpression
  auto required_input_props = gexpr->GetInputProperties(required_props);
  TERRIER_ASSERT(required_input_props.size() == child_groups.size(),
                 "input properties and group size mismatch");

  // Firstly derive input/output columns
  InputColumnDeriver deriver;
  auto output_input_cols_pair = deriver.DeriveInputColumns(
    gexpr, required_props, required_cols, &metadata_.memo
  );

  auto &output_cols = output_input_cols_pair.first;
  auto &input_cols = output_input_cols_pair.second;
  TERRIER_ASSERT(input_cols.size() == required_input_props.size(),
                 "input columns and input properties size mismatch");

  // Derive chidren plans first because they are useful in the derivation of
  // root plan. Also keep propagate expression to column offset mapping
  std::vector<planner::AbstractPlanNode*> children_plans;
  std::vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    for (unsigned offset = 0; offset < input_cols[i].size(); ++offset) {
      TERRIER_ASSERT(input_cols[i][offset] != nullptr, "invalid input column found");
      child_expr_map[input_cols[i][offset]] = offset;
    }

    auto child_plan = ChooseBestPlan(child_groups[i], required_input_props[i], input_cols[i]);
    TERRIER_ASSERT(child_plan != nullptr, "child should have derived a non-null plan...");

    children_plans.push_back(child_plan);
    children_expr_map.push_back(child_expr_map);
  }

  // Derive root plan
  OperatorExpression *op = new OperatorExpression(gexpr->Op(), {});

  PlanGenerator generator;
  auto plan = generator.ConvertOpExpression(op, required_props, required_cols,
                                            output_cols, children_plans,
                                            children_expr_map, group->GetNumRows());
  OPTIMIZER_LOG_TRACE("Finish Choosing best plan for group %d", id);
  return plan;
}

void Optimizer::OptimizeLoop(
    int root_group_id,
    PropertySet* required_props,
    settings::SettingsManager *settings) {

  OptimizeContext* root_context = new OptimizeContext(&metadata_, required_props);
  auto task_stack = new OptimizerTaskStack();
  metadata_.SetTaskPool(task_stack);
  metadata_.track_list.push_back(root_context);

  // Perform rewrite first
  task_stack->Push(new TopDownRewrite(root_group_id, root_context, RewriteRuleSetName::PREDICATE_PUSH_DOWN));
  task_stack->Push(new BottomUpRewrite(root_group_id, root_context, RewriteRuleSetName::UNNEST_SUBQUERY, false));
  ExecuteTaskStack(task_stack, root_group_id, root_context, settings);

  // Perform optimization after the rewrite
  task_stack->Push(new OptimizeGroup(metadata_.memo.GetGroupByID(root_group_id), root_context));

  // Derive stats for the only one logical expression before optimizing
  task_stack->Push(new DeriveStats(metadata_.memo.GetGroupByID(root_group_id)->GetLogicalExpression(), ExprSet{}, root_context));
  ExecuteTaskStack(task_stack, root_group_id, root_context, settings);
}

void Optimizer::ExecuteTaskStack(
    OptimizerTaskStack *task_stack,
    int root_group_id,
    OptimizeContext* root_context,
    settings::SettingsManager *settings) {

  auto root_group = metadata_.memo.GetGroupByID(root_group_id);
  const auto timeout_limit = static_cast<uint64_t>(settings->GetInt(settings::Param::task_execution_timeout));
  const auto &required_props = root_context->required_prop;

  uint64_t elapsed_time = 0;

  // Iterate through the task stack
  while (!task_stack->Empty()) {
    // Check to see if we have at least one plan, and if we have exceeded our
    // timeout limit
    if (elapsed_time >= timeout_limit && root_group->HasExpressions(required_props)) {
      throw OPTIMIZER_EXCEPTION("Optimizer task execution timed out");
    }

    uint64_t task_runtime = 0;
    auto task = task_stack->Pop();
    {
      common::ScopedTimer timer(&task_runtime);
      task->execute();
    }
    elapsed_time += task_runtime;
  }
}

}  // namespace optimizer
}  // namespace terrier
