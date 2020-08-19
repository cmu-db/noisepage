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
#include "planner/plannodes/cte_scan_plan_node.h"

namespace terrier::optimizer {

void Optimizer::Reset() { context_ = std::make_unique<OptimizerContext>(common::ManagedPointer(cost_model_)); }

std::unique_ptr<planner::AbstractPlanNode> Optimizer::BuildPlanTree(transaction::TransactionContext *txn,
                                                                    catalog::CatalogAccessor *accessor,
                                                                    StatsStorage *storage, QueryInfo query_info,
                                                                    std::unique_ptr<AbstractOptimizerNode> op_tree) {
  context_->SetTxn(txn);
  context_->SetCatalogAccessor(accessor);
  context_->SetStatsStorage(storage);

  // Generate initial operator tree from query tree
  GroupExpression *gexpr = nullptr;
  UNUSED_ATTRIBUTE bool insert = context_->RecordOptimizerNodeIntoGroup(common::ManagedPointer(op_tree), &gexpr);
  TERRIER_ASSERT(insert && gexpr, "Logical expression tree should insert");

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
    auto best_plan = ChooseBestPlan(txn, accessor, root_id, phys_properties, output_exprs);

    // Assign CTE Schema to each CTE Node
    for (auto &table : context_->GetCTETables()) {
      planner::CteScanPlanNode *leader = nullptr;
      common::ManagedPointer<planner::CteScanPlanNode> ldr = common::ManagedPointer(leader);
      ElectCTELeader(common::ManagedPointer(best_plan), table, &ldr);
    }

    // Reset memo after finishing the optimization
    Reset();
    return best_plan;
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

void Optimizer::ElectCTELeader(common::ManagedPointer<planner::AbstractPlanNode> plan, catalog::table_oid_t table_oid,
                               common::ManagedPointer<planner::CteScanPlanNode> *leader) {
  if ((plan->GetPlanNodeType() == planner::PlanNodeType::CTESCAN) &&
      (plan.CastManagedPointerTo<planner::CteScanPlanNode>()->GetTableOid() == table_oid)) {
    if (plan->GetChildren().empty()) {
      // Set cte schema
      auto cte_scan_plan_node_set = dynamic_cast<planner::CteScanPlanNode *>(plan.Get());
      cte_scan_plan_node_set->SetTableSchema(context_->GetCTESchema(table_oid));
      cte_scan_plan_node_set->SetLeader(leader->CastManagedPointerTo<const planner::CteScanPlanNode>());
    } else {
      // Child bearing CTE node
      // Replace with leader
      if (*leader != nullptr) {
        std::vector<std::unique_ptr<planner::AbstractPlanNode>> adopted_children;
        plan->MoveChildren(&adopted_children);
        TERRIER_ASSERT(adopted_children.size() == 1, "CTE leader should have 1 child");
        (*leader)->AddChild(std::move(adopted_children[0]));
        auto cte_scan = dynamic_cast<planner::CteScanPlanNode *>(plan.Get());
        cte_scan->SetLeader(leader->CastManagedPointerTo<const planner::CteScanPlanNode>());
      }
    }

    if (*leader == nullptr) {
      auto current_cte = dynamic_cast<planner::CteScanPlanNode *>(plan.Get());

      std::vector<planner::OutputSchema::Column> table_columns;
      for(auto &col : context_->GetCTESchema(table_oid).GetColumns()){
        table_columns.emplace_back(col.Name(), col.Type(), std::make_unique<parser::ColumnValueExpression>(
            current_cte->GetCTETableName(), col.Name(), catalog::INVALID_DATABASE_OID,
            table_oid, col.Oid(), col.Type()));
      }
      auto new_output_schema = std::make_unique<planner::OutputSchema>(std::move(table_columns));

      auto builder = planner::CteScanPlanNode::Builder();
      builder.SetLeader(true)
          .SetScanPredicate(current_cte->GetScanPredicate())
          .SetCTEType(current_cte->GetCTEType())
          .SetCTETableName(std::string(current_cte->GetCTETableName()))
          .SetOutputSchema(std::move(new_output_schema))
          .SetTableSchema(catalog::Schema(*current_cte->GetTableSchema().Get()))
          .SetTableOid(table_oid);
      std::vector<std::unique_ptr<planner::AbstractPlanNode>> children;
      current_cte->MoveChildren(&children);
      //      for(auto &child : children){
      //        builder.AddChild(std::move(child));
      //      }

//      TERRIER_ASSERT(children.size() > 0, "Nothing to fill the leader with???");
      for(size_t i = 0;i < children.size();i++){
        builder.AddChild(std::move(children[i]));
      }

      auto new_leader = builder.Build();
      *leader = common::ManagedPointer(new_leader);
      current_cte->AddChild(std::move(new_leader));

      current_cte->SetLeader(leader->CastManagedPointerTo<const planner::CteScanPlanNode>());
    }
  }
  auto children = plan->GetChildren();
  for (auto &i : children) {
    ElectCTELeader(i, table_oid, leader);
  }
}

std::unique_ptr<planner::AbstractPlanNode> Optimizer::ChooseBestPlan(
    transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, group_id_t id,
    PropertySet *required_props, const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols) {
  Group *group = context_->GetMemo().GetGroupByID(id);
  auto gexpr = group->GetBestExpression(required_props);

  OPTIMIZER_LOG_TRACE("Choosing best plan for group " + std::to_string(!gexpr->GetGroupID()) + " with op " +
                      gexpr->Contents()->GetName());

  std::vector<group_id_t> child_groups = gexpr->GetChildGroupIDs();

  // required_input_props is owned by the GroupExpression
  auto required_input_props = gexpr->GetInputProperties(required_props);

  // Firstly derive input/output columns
  InputColumnDeriver deriver(txn, accessor);
  auto output_input_cols_pair = deriver.DeriveInputColumns(gexpr, required_props, required_cols, &context_->GetMemo());

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

    auto child_plan = ChooseBestPlan(txn, accessor, child_groups[i], required_input_props[i], input_cols[i]);
    TERRIER_ASSERT(child_plan != nullptr, "child should have derived a non-null plan...");

    children_plans.emplace_back(std::move(child_plan));
    children_expr_map.push_back(child_expr_map);
  }

  // Derive root plan
  auto *op = new OperatorNode(gexpr->Contents(), {}, txn);

  PlanGenerator generator;
  auto plan = generator.ConvertOpNode(txn, accessor, op, required_props, required_cols, output_cols,
                                      std::move(children_plans), std::move(children_expr_map));
  OPTIMIZER_LOG_TRACE("Finish Choosing best plan for group " + std::to_string(!id));

  if (op->Contents()->GetOpType() == OpType::CTESCAN && !child_groups.empty()) {
    TERRIER_ASSERT(child_groups.size() <= 2, "CTE should not have more than 2 children.");
    if (plan->GetPlanNodeType() == planner::PlanNodeType::CTESCAN) {
      auto cte_scan_plan_node = reinterpret_cast<planner::CteScanPlanNode *>(plan.get());
      context_->SetCTESchema(cte_scan_plan_node->GetTableOid(), *cte_scan_plan_node->GetTableSchema());
    } else if ((*plan->GetChildren().begin())->GetPlanNodeType() == planner::PlanNodeType::CTESCAN) {
      // CTE Scan node can be inside a Projection node
      auto cte_scan_plan_node = reinterpret_cast<planner::CteScanPlanNode *>(&(*plan->GetChildren().begin()->Get()));
      context_->SetCTESchema(cte_scan_plan_node->GetTableOid(), *(cte_scan_plan_node)->GetTableSchema());
    }
  }

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
  task_stack->Push(new DeriveStats(memo.GetGroupByID(root_group_id)->GetLogicalExpression(), ExprSet{}, root_context));

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

}  // namespace terrier::optimizer
