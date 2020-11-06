#include "optimizer/optimizer_task.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <vector>

#include "loggers/optimizer_logger.h"
#include "optimizer/binding.h"
#include "optimizer/child_property_deriver.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/statistics/child_stats_deriver.h"
#include "optimizer/statistics/stats_calculator.h"

namespace noisepage::optimizer {

//===--------------------------------------------------------------------===//
// Base class
//===--------------------------------------------------------------------===//
void OptimizerTask::ConstructValidRules(GroupExpression *group_expr, const std::vector<Rule *> &rules,
                                        std::vector<RuleWithPromise> *valid_rules) {
  for (auto &rule : rules) {
    // Check if we can apply the rule
    bool root_pattern_mismatch = group_expr->Contents()->GetOpType() != rule->GetMatchPattern()->Type();
    bool already_explored = group_expr->HasRuleExplored(rule);

    // This check exists only as an "early" reject. As is evident, we do not check
    // the full patern here. Checking the full pattern happens when actually trying to
    // apply the rule (via a GroupExprBindingIterator).
    bool child_pattern_mismatch =
        group_expr->GetChildrenGroupsSize() != rule->GetMatchPattern()->GetChildPatternsSize();

    if (root_pattern_mismatch || already_explored || child_pattern_mismatch) {
      continue;
    }

    auto promise = rule->Promise(group_expr);
    if (promise != RulePromise::NO_PROMISE) valid_rules->emplace_back(rule, promise);
  }
}

void OptimizerTask::PushTask(OptimizerTask *task) { context_->GetOptimizerContext()->PushTask(task); }

Memo &OptimizerTask::GetMemo() const { return context_->GetOptimizerContext()->GetMemo(); }

RuleSet &OptimizerTask::GetRuleSet() const { return context_->GetOptimizerContext()->GetRuleSet(); }

//===--------------------------------------------------------------------===//
// OptimizeGroup
//===--------------------------------------------------------------------===//
void OptimizeGroup::Execute() {
  OPTIMIZER_LOG_TRACE("OptimizeGroup::Execute() group " + std::to_string(group_->GetID().UnderlyingValue()));
  if (group_->GetCostLB() > context_->GetCostUpperBound() ||                    // Cost LB > Cost UB
      group_->GetBestExpression(context_->GetRequiredProperties()) != nullptr)  // Has optimized given the context
    return;

  // Push explore task first for logical expressions if the group has not been explored
  if (!group_->HasExplored()) {
    for (auto &logical_expr : group_->GetLogicalExpressions()) {
      PushTask(new OptimizeExpression(logical_expr, context_));
    }
  }

  // Push implement tasks to ensure that they are run first (for early pruning)
  for (auto &physical_expr : group_->GetPhysicalExpressions()) {
    PushTask(new OptimizeExpressionCostWithEnforcedProperty(physical_expr, context_));
  }

  // Since there is no cycle in the tree, it is safe to set the flag even before
  // all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// OptimizeExpression
//===--------------------------------------------------------------------===//
void OptimizeExpression::Execute() {
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  auto logical_rules = GetRuleSet().GetRulesByName(RuleSetName::LOGICAL_TRANSFORMATION);
  auto phys_rules = GetRuleSet().GetRulesByName(RuleSetName::PHYSICAL_IMPLEMENTATION);
  ConstructValidRules(group_expr_, logical_rules, &valid_rules);
  ConstructValidRules(group_expr_, phys_rules, &valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());
  OPTIMIZER_LOG_DEBUG("OptimizeExpression::execute() op {0}, valid rules : {1}",
                      static_cast<int>(group_expr_->Contents()->GetOpType()), valid_rules.size());
  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.GetRule(), context_));
    int child_group_idx = 0;
    for (auto &child_pattern : r.GetRule()->GetMatchPattern()->Children()) {
      // If child_pattern has any more children (i.e non-leaf), then we will explore the
      // child before applying the rule. (assumes task pool is effectively a stack)
      if (child_pattern->GetChildPatternsSize() > 0) {
        Group *group = GetMemo().GetGroupByID(group_expr_->GetChildGroupIDs()[child_group_idx]);
        PushTask(new ExploreGroup(group, context_));
      }

      child_group_idx++;
    }
  }
}

//===--------------------------------------------------------------------===//
// ExploreGroup
//===--------------------------------------------------------------------===//
void ExploreGroup::Execute() {
  if (group_->HasExplored()) return;
  OPTIMIZER_LOG_TRACE("ExploreGroup::Execute() ");

  for (auto &logical_expr : group_->GetLogicalExpressions()) {
    PushTask(new ExploreExpression(logical_expr, context_));
  }

  // Since there is no cycle in the tree, it is safe to set the flag even before
  // all expressions are explored
  group_->SetExplorationFlag();
}

//===--------------------------------------------------------------------===//
// ExploreExpression
//===--------------------------------------------------------------------===//
void ExploreExpression::Execute() {
  OPTIMIZER_LOG_TRACE("ExploreExpression::Execute() ");
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  auto logical_rules = GetRuleSet().GetRulesByName(RuleSetName::LOGICAL_TRANSFORMATION);
  ConstructValidRules(group_expr_, logical_rules, &valid_rules);
  std::sort(valid_rules.begin(), valid_rules.end());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.GetRule(), context_, true));
    int child_group_idx = 0;
    for (auto &child_pattern : r.GetRule()->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the
      // current group. this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        Group *group = GetMemo().GetGroupByID(group_expr_->GetChildGroupIDs()[child_group_idx]);
        PushTask(new ExploreGroup(group, context_));
      }

      child_group_idx++;
    }
  }
}

//===--------------------------------------------------------------------===//
// ApplyRule
//===--------------------------------------------------------------------===//
void ApplyRule::Execute() {
  OPTIMIZER_LOG_TRACE("ApplyRule::Execute() for rule: {0}", rule_->GetRuleIdx());
  if (group_expr_->HasRuleExplored(rule_)) return;

  GroupExprBindingIterator iterator(GetMemo(), group_expr_, rule_->GetMatchPattern(),
                                    context_->GetOptimizerContext()->GetTxn());
  while (iterator.HasNext()) {
    auto before = iterator.Next();
    if (!rule_->Check(common::ManagedPointer(before.get()), context_)) {
      continue;
    }

    // Caller frees after
    std::vector<std::unique_ptr<AbstractOptimizerNode>> after;
    rule_->Transform(common::ManagedPointer(before.get()), &after, context_);
    for (const auto &new_expr : after) {
      GroupExpression *new_gexpr = nullptr;
      auto g_id = group_expr_->GetGroupID();
      if (context_->GetOptimizerContext()->RecordOptimizerNodeIntoGroup(common::ManagedPointer(new_expr), &new_gexpr,
                                                                        g_id)) {
        // A new group expression is generated
        if (new_gexpr->Contents()->IsLogical()) {
          // Derive stats for the *logical expression*
          PushTask(new DeriveStats(new_gexpr, ExprSet{}, context_));
          if (explore_only_) {
            // Explore this logical expression
            PushTask(new ExploreExpression(new_gexpr, context_));
          } else {
            // Optimize this logical expression
            PushTask(new OptimizeExpression(new_gexpr, context_));
          }
        } else {
          // Cost this physical expression and optimize its inputs
          PushTask(new OptimizeExpressionCostWithEnforcedProperty(new_gexpr, context_));
        }
      }
    }
  }

  group_expr_->SetRuleExplored(rule_);
}

//===--------------------------------------------------------------------===//
// DeriveStats
//===--------------------------------------------------------------------===//
void DeriveStats::Execute() {
  // First do a top-down pass to get stats for required columns, then do a
  // bottom-up pass to calculate the stats
  ChildStatsDeriver deriver;
  auto children_required_stats =
      deriver.DeriveInputStats(gexpr_, required_cols_, &context_->GetOptimizerContext()->GetMemo());
  bool derive_children = false;
  OPTIMIZER_LOG_TRACE("DeriveStats::Execute() group " + std::to_string(gexpr_->GetGroupID().UnderlyingValue()));

  // If we haven't got enough stats to compute the current stats, derive them
  // from the child first
  NOISEPAGE_ASSERT(children_required_stats.size() == gexpr_->GetChildrenGroupsSize(), "Stats size mismatch");
  for (size_t idx = 0; idx < children_required_stats.size(); ++idx) {
    auto &child_required_stats = children_required_stats[idx];
    auto child_group_id = gexpr_->GetChildGroupId(static_cast<int>(idx));
    // TODO(boweic): currently we pick the first child expression in the child
    // group to derive stats, in the future we may want to pick the one with
    // the highest confidence
    auto child_group_gexpr = GetMemo().GetGroupByID(child_group_id)->GetLogicalExpressions()[0];
    if (!child_required_stats.empty() || !child_group_gexpr->HasDerivedStats()) {
      // The child group has not derived stats could happen when we do top-down
      // stats derivation for the first time or a new child group is just
      // generated by join order enumeration
      if (!derive_children) {
        derive_children = true;
        // Derive stats for root later
        PushTask(new DeriveStats(this));
      }
      PushTask(new DeriveStats(child_group_gexpr, child_required_stats, context_));
    }
  }

  if (derive_children) {
    // We'll derive for the current group after deriving stats of children
    return;
  }

  StatsCalculator calculator;
  calculator.CalculateStats(gexpr_, required_cols_, context_->GetOptimizerContext());
  gexpr_->SetDerivedStats();
}

//===--------------------------------------------------------------------===//
// OptimizeExpressionCostWithEnforcedProperty
//===--------------------------------------------------------------------===//
void OptimizeExpressionCostWithEnforcedProperty::Execute() {
  // Init logic: only run once per task
  OPTIMIZER_LOG_TRACE("OptimizeExpressionCostWithEnforcedProperty::Execute() ");
  if (cur_child_idx_ == -1) {
    // TODO(patrick):
    // 1. We can init input cost using non-zero value for pruning
    // 2. We can calculate the current operator cost if we have maintain
    //    logical properties in group (e.g. stats, schema, cardinality)
    cur_total_cost_ = 0;

    // Pruning
    if (cur_total_cost_ > context_->GetCostUpperBound()) return;

    // Derive output and input properties
    ChildPropertyDeriver prop_deriver;
    output_input_properties_ = prop_deriver.GetProperties(context_->GetOptimizerContext()->GetCatalogAccessor(),
                                                          &context_->GetOptimizerContext()->GetMemo(),
                                                          context_->GetRequiredProperties(), group_expr_);
    cur_child_idx_ = 0;

    // TODO(patrick/boweic): If later on we support properties that may not be enforced in some
    // cases, we can check whether it is the case here to do the pruning
  }

  // Loop over (output prop, input props) pair for the GroupExpression being optimized
  // (1) Cost children (if needed); or pick the best child expression (in terms of cost)
  // (2) Enforce any missing properties as required
  // (3) Update Group/Context metadata of expression + cost
  for (; cur_prop_pair_idx_ < static_cast<int>(output_input_properties_.size()); cur_prop_pair_idx_++) {
    auto &output_prop = output_input_properties_[cur_prop_pair_idx_].first;
    auto &input_props = output_input_properties_[cur_prop_pair_idx_].second;

    // Calculate local cost and update total cost
    if (cur_child_idx_ == 0) {
      // Compute the cost of the root operator
      // 1. Collect stats needed and cache them in the group
      // 2. Calculate cost based on children's stats
      cur_total_cost_ += context_->GetOptimizerContext()->GetCostModel()->CalculateCost(
          context_->GetOptimizerContext()->GetTxn(), context_->GetOptimizerContext()->GetCatalogAccessor(),
          &context_->GetOptimizerContext()->GetMemo(), group_expr_);
    }

    for (; cur_child_idx_ < static_cast<int>(group_expr_->GetChildrenGroupsSize()); cur_child_idx_++) {
      auto &i_prop = input_props[cur_child_idx_];
      auto child_group =
          context_->GetOptimizerContext()->GetMemo().GetGroupByID(group_expr_->GetChildGroupId(cur_child_idx_));

      // Check whether the child group is already optimized for the prop
      auto child_best_expr = child_group->GetBestExpression(i_prop);
      if (child_best_expr != nullptr) {  // Directly get back the best expr if the child group is optimized
        cur_total_cost_ += child_best_expr->GetCost(i_prop);
        if (cur_total_cost_ > context_->GetCostUpperBound()) break;
      } else if (prev_child_idx_ != cur_child_idx_) {  // We haven't optimized child group
        prev_child_idx_ = cur_child_idx_;
        PushTask(new OptimizeExpressionCostWithEnforcedProperty(this));

        auto cost_high = context_->GetCostUpperBound() - cur_total_cost_;
        auto ctx = new OptimizationContext(context_->GetOptimizerContext(), i_prop->Copy(), cost_high);
        PushTask(new OptimizeGroup(child_group, ctx));
        context_->GetOptimizerContext()->AddOptimizationContext(ctx);
        return;
      } else {  // If we return from OptimizeGroup, then there is no expr for the context
        break;
      }
    }

    // TODO(wz2): Can we reduce the amount of copying
    // Check whether we successfully optimize all child group
    if (cur_child_idx_ == static_cast<int>(group_expr_->GetChildrenGroupsSize())) {
      // Not need to do pruning here because it has been done when we get the
      // best expr from the child group

      // Add this group expression to group expression hash table
      std::vector<PropertySet *> input_props_copy;
      input_props_copy.reserve(input_props.size());
      for (auto i_prop : input_props) {
        input_props_copy.push_back(i_prop->Copy());
      }

      group_expr_->SetLocalHashTable(output_prop->Copy(), input_props_copy, cur_total_cost_);
      auto cur_group = GetMemo().GetGroupByID(group_expr_->GetGroupID());
      cur_group->SetExpressionCost(group_expr_, cur_total_cost_, output_prop->Copy());

      // Enforce property if the requirement does not meet
      PropertyEnforcer prop_enforcer;
      GroupExpression *memo_enforced_expr = nullptr;
      bool meet_requirement = true;

      // TODO(patrick/boweic): For now, we enforce the missing properties in the order of how we
      // find them. This may miss the opportunity to enforce them or may lead to
      // sub-optimal plan. This is fine now because we only have one physical
      // property (sort). If more properties are added, we should add some heuristics
      // to derive the optimal enforce order or perform a cost-based full enumeration.
      for (auto &prop : context_->GetRequiredProperties()->Properties()) {
        if (!output_prop->HasProperty(*prop)) {
          auto enforced_expr =
              prop_enforcer.EnforceProperty(group_expr_, prop, context_->GetOptimizerContext()->GetTxn());
          // Cannot enforce the missing property
          if (enforced_expr == nullptr) {
            meet_requirement = false;
            break;
          }

          memo_enforced_expr = GetMemo().InsertExpression(enforced_expr, group_expr_->GetGroupID(), true);

          // Extend the output properties after enforcement
          auto pre_output_prop_set = output_prop->Copy();

          // Cost the enforced expression
          auto extended_prop_set = output_prop->Copy();
          extended_prop_set->AddProperty(prop->Copy());
          cur_total_cost_ += context_->GetOptimizerContext()->GetCostModel()->CalculateCost(
              context_->GetOptimizerContext()->GetTxn(), context_->GetOptimizerContext()->GetCatalogAccessor(),
              &context_->GetOptimizerContext()->GetMemo(), memo_enforced_expr);

          // Update hash tables for group and group expression
          memo_enforced_expr->SetLocalHashTable(extended_prop_set, {pre_output_prop_set}, cur_total_cost_);
          cur_group->SetExpressionCost(memo_enforced_expr, cur_total_cost_, output_prop->Copy());
        }
      }

      // Can meet the requirement
      if (meet_requirement && cur_total_cost_ <= context_->GetCostUpperBound()) {
        // If the cost is smaller than the winner, update the context upper bound
        context_->SetCostUpperBound(context_->GetCostUpperBound() - cur_total_cost_);
        if (memo_enforced_expr != nullptr) {  // Enforcement takes place
          cur_group->SetExpressionCost(memo_enforced_expr, cur_total_cost_, context_->GetRequiredProperties()->Copy());
        } else if (output_prop->Properties().size() != context_->GetRequiredProperties()->Properties().size()) {
          // The original output property is a super set of the requirement
          cur_group->SetExpressionCost(group_expr_, cur_total_cost_, context_->GetRequiredProperties()->Copy());
        }
      }
    }

    // Reset child idx and total cost
    prev_child_idx_ = -1;
    cur_child_idx_ = 0;
    cur_total_cost_ = 0;
  }
}

void TopDownRewrite::Execute() {
  std::vector<RuleWithPromise> valid_rules;

  auto cur_group = GetMemo().GetGroupByID(group_id_);
  auto cur_group_expr = cur_group->GetLogicalExpression();

  // Construct valid transformation rules from rule set
  std::vector<Rule *> set = GetRuleSet().GetRulesByName(rule_set_name_);
  ConstructValidRules(cur_group_expr, set, &valid_rules);

  // Sort so that we apply rewrite rules with higher promise first
  std::sort(valid_rules.begin(), valid_rules.end(), std::greater<>());

  for (auto &r : valid_rules) {
    Rule *rule = r.GetRule();
    GroupExprBindingIterator iterator(GetMemo(), cur_group_expr, rule->GetMatchPattern(),
                                      context_->GetOptimizerContext()->GetTxn());
    if (iterator.HasNext()) {
      auto before = iterator.Next();
      NOISEPAGE_ASSERT(!iterator.HasNext(), "there should only be 1 binding");
      std::vector<std::unique_ptr<AbstractOptimizerNode>> after;
      rule->Transform(common::ManagedPointer(before.get()), &after, context_);

      // Rewrite rule should provide at most 1 expression
      NOISEPAGE_ASSERT(after.size() <= 1, "rule provided too many transformations");
      if (!after.empty()) {
        auto &new_expr = after[0];
        context_->GetOptimizerContext()->ReplaceRewriteExpression(common::ManagedPointer(new_expr.get()), group_id_);
        PushTask(new TopDownRewrite(group_id_, context_, rule_set_name_));
        return;
      }
    }
    cur_group_expr->SetRuleExplored(rule);
  }

  size_t size = cur_group_expr->GetChildrenGroupsSize();
  for (size_t child_group_idx = 0; child_group_idx < size; child_group_idx++) {
    // Need to rewrite all sub trees first
    auto id = cur_group_expr->GetChildGroupId(static_cast<int>(child_group_idx));
    auto task = new TopDownRewrite(id, context_, rule_set_name_);
    PushTask(task);
  }
}

void BottomUpRewrite::Execute() {
  std::vector<RuleWithPromise> valid_rules;

  auto cur_group = GetMemo().GetGroupByID(group_id_);
  auto cur_group_expr = cur_group->GetLogicalExpression();

  if (!has_optimized_child_) {
    PushTask(new BottomUpRewrite(group_id_, context_, rule_set_name_, true));

    size_t size = cur_group_expr->GetChildrenGroupsSize();
    for (size_t child_group_idx = 0; child_group_idx < size; child_group_idx++) {
      // Need to rewrite all sub trees first
      auto id = cur_group_expr->GetChildGroupId(static_cast<int>(child_group_idx));
      auto task = new BottomUpRewrite(id, context_, rule_set_name_, false);
      PushTask(task);
    }
    return;
  }

  // Construct valid transformation rules from rule set
  std::vector<Rule *> set = GetRuleSet().GetRulesByName(rule_set_name_);
  ConstructValidRules(cur_group_expr, set, &valid_rules);

  // Sort so that we apply rewrite rules with higher promise first
  std::sort(valid_rules.begin(), valid_rules.end(), std::greater<>());

  for (auto &r : valid_rules) {
    Rule *rule = r.GetRule();
    GroupExprBindingIterator iterator(GetMemo(), cur_group_expr, rule->GetMatchPattern(),
                                      context_->GetOptimizerContext()->GetTxn());
    if (iterator.HasNext()) {
      auto before = iterator.Next();
      NOISEPAGE_ASSERT(!iterator.HasNext(), "should only bind to 1");
      std::vector<std::unique_ptr<AbstractOptimizerNode>> after;
      rule->Transform(common::ManagedPointer(before.get()), &after, context_);

      // Rewrite rule should provide at most 1 expression
      NOISEPAGE_ASSERT(after.size() <= 1, "rule generated too many transformations");
      // If a rule is applied, we replace the old expression and optimize this
      // group again, this will ensure that we apply rule for this level until
      // saturated, also children are already been rewritten
      if (!after.empty()) {
        auto &new_expr = after[0];
        context_->GetOptimizerContext()->ReplaceRewriteExpression(common::ManagedPointer(new_expr.get()), group_id_);
        PushTask(new BottomUpRewrite(group_id_, context_, rule_set_name_, false));
        return;
      }
    }

    cur_group_expr->SetRuleExplored(rule);
  }
}

}  // namespace noisepage::optimizer
