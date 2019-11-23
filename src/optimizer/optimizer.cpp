#include <memory>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/scoped_timer.h"

#include "optimizer/binding.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimize_context.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/plan_generator.h"
#include "optimizer/properties.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::optimizer {

void Optimizer::Reset() {
  // cleanup any existing resources;
  delete metadata_;
  metadata_ = new OptimizerMetadata(cost_model_);
}

std::unique_ptr<planner::AbstractPlanNode> Optimizer::BuildPlanTree(
    std::unique_ptr<OperatorExpression> op_tree, QueryInfo query_info, transaction::TransactionContext *txn,
    settings::SettingsManager *settings, catalog::CatalogAccessor *accessor, StatsStorage *storage) {
  metadata_->SetTxn(txn);
  metadata_->SetCatalogAccessor(accessor);
  metadata_->SetStatsStorage(storage);

  // Generate initial operator tree from query tree
  GroupExpression *gexpr = nullptr;
  UNUSED_ATTRIBUTE bool insert = metadata_->RecordTransformedExpression(common::ManagedPointer(op_tree), &gexpr);
  TERRIER_ASSERT(insert && gexpr, "Logical expression tree should insert");

  GroupID root_id = gexpr->GetGroupID();

  // Physical properties
  PropertySet *phys_properties = query_info.GetPhysicalProperties();

  // Give raw pointers to ChooseBestPlan
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_exprs;
  for (auto expr : query_info.GetOutputExprs()) {
    output_exprs.push_back(expr);
  }

  try {
    OptimizeLoop(root_id, phys_properties, settings);
  } catch (OptimizerException &e) {
    OPTIMIZER_LOG_WARN("Optimize Loop ended prematurely: {0}", e.what());
  }

  try {
    auto best_plan = ChooseBestPlan(root_id, phys_properties, output_exprs, settings, accessor, txn);

    // Reset memo after finishing the optimization
    Reset();
    return best_plan;
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

std::unique_ptr<planner::AbstractPlanNode> Optimizer::ChooseBestPlan(
    GroupID id, PropertySet *required_props,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
    settings::SettingsManager *settings, catalog::CatalogAccessor *accessor, transaction::TransactionContext *txn) {
  Group *group = metadata_->GetMemo().GetGroupByID(id);
  auto gexpr = group->GetBestExpression(required_props);

  OPTIMIZER_LOG_TRACE("Choosing best plan for group {0} with op {1}", gexpr->GetGroupID(),
                      gexpr->Op().GetName().c_str());

  std::vector<GroupID> child_groups = gexpr->GetChildGroupIDs();

  // required_input_props is owned by the GroupExpression
  auto required_input_props = gexpr->GetInputProperties(required_props);
  TERRIER_ASSERT(required_input_props.size() == child_groups.size(), "input properties and group size mismatch");

  // Firstly derive input/output columns
  InputColumnDeriver deriver(accessor, txn);
  auto output_input_cols_pair = deriver.DeriveInputColumns(gexpr, required_props, required_cols, &metadata_->GetMemo());

  auto &output_cols = output_input_cols_pair.first;
  auto &input_cols = output_input_cols_pair.second;
  TERRIER_ASSERT(input_cols.size() == required_input_props.size(), "input columns and input properties size mismatch");

  // Derive chidren plans first because they are useful in the derivation of
  // root plan. Also keep propagate expression to column offset mapping
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans;
  std::vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    for (unsigned offset = 0; offset < input_cols[i].size(); ++offset) {
      TERRIER_ASSERT(input_cols[i][offset] != nullptr, "invalid input column found");
      child_expr_map[input_cols[i][offset]] = offset;
    }

    auto child_plan = ChooseBestPlan(child_groups[i], required_input_props[i], input_cols[i], settings, accessor, txn);
    TERRIER_ASSERT(child_plan != nullptr, "child should have derived a non-null plan...");

    children_plans.emplace_back(std::move(child_plan));
    children_expr_map.push_back(child_expr_map);
  }

  // Derive root plan
  OperatorExpression *op = new OperatorExpression(Operator(gexpr->Op()), {});

  PlanGenerator generator;
  auto plan = generator.ConvertOpExpression(op, required_props, required_cols, output_cols, std::move(children_plans),
                                            std::move(children_expr_map), settings, accessor, txn);
  OPTIMIZER_LOG_TRACE("Finish Choosing best plan for group {0}", id);

  delete op;
  return plan;
}

void Optimizer::OptimizeLoop(int root_group_id, PropertySet *required_props, settings::SettingsManager *settings) {
  auto root_context = new OptimizeContext(metadata_, required_props->Copy());
  auto task_stack = new OptimizerTaskStack();
  metadata_->SetTaskPool(task_stack);
  metadata_->AddOptimizeContext(root_context);

  // Perform rewrite first
  task_stack->Push(new TopDownRewrite(root_group_id, root_context, RewriteRuleSetName::PREDICATE_PUSH_DOWN));
  task_stack->Push(new BottomUpRewrite(root_group_id, root_context, RewriteRuleSetName::UNNEST_SUBQUERY, false));
  ExecuteTaskStack(task_stack, root_group_id, root_context, settings);

  // Perform optimization after the rewrite
  Memo &memo = metadata_->GetMemo();
  task_stack->Push(new OptimizeGroup(memo.GetGroupByID(root_group_id), root_context));

  // Derive stats for the only one logical expression before optimizing
  task_stack->Push(new DeriveStats(memo.GetGroupByID(root_group_id)->GetLogicalExpression(), ExprSet{}, root_context));
  ExecuteTaskStack(task_stack, root_group_id, root_context, settings);
}

void Optimizer::ExecuteTaskStack(OptimizerTaskStack *task_stack, int root_group_id, OptimizeContext *root_context,
                                 settings::SettingsManager *settings) {
  auto root_group = metadata_->GetMemo().GetGroupByID(root_group_id);
  const auto timeout_limit = static_cast<uint64_t>(settings->GetInt(settings::Param::task_execution_timeout));
  const auto &required_props = root_context->GetRequiredProperties();

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
      common::ScopedTimer<std::chrono::milliseconds> timer(&task_runtime);
      task->Execute();
    }
    delete task;
    elapsed_time += task_runtime;
  }
}

}  // namespace terrier::optimizer
