#pragma once

#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/property_set.h"

namespace terrier {
namespace parser {
class AbstractExpression;
}

namespace optimizer {

class OptimizeContext;
class Memo;
class Rule;
struct RuleWithPromise;
class RuleSet;
class Group;
class GroupExpression;
class OptimizerMetadata;
enum class RewriteRuleSetName : uint32_t;

/**
 * Enumeration defining the various optimizer task types
 */
enum class OptimizerTaskType {
  OPTIMIZE_GROUP,
  OPTIMIZE_EXPR,
  EXPLORE_GROUP,
  EXPLORE_EXPR,
  APPLY_RULE,
  OPTIMIZE_INPUTS,
  DERIVE_STATS,
  REWRITE_EXPR,
  APPLY_REWIRE_RULE,
  TOP_DOWN_REWRITE,
  BOTTOM_UP_REWRITE
};

/**
 * OptimizerTask is the base abstract class for optimization
 */
class OptimizerTask {
 public:
  /**
   * Constructor for OptimizerTask
   * @param context OptimizeContext for current optimization
   * @param type Type of the optimization task
   */
  OptimizerTask(OptimizeContext* context,
                OptimizerTaskType type)
      : type_(type), context_(context) {}

  /**
   * Construct valid rules with their promises for a group expression,
   * promises are used to determine the order of applying the rules. We
   * currently use the promise to enforce that physical rules to be applied
   * before logical rules
   *
   * @param group_expr The group expressions to apply rules
   * @param context The current optimize context
   * @param rules The candidate rule set
   * @param valid_rules The valid rules to apply in the current rule set will be
   *  append to valid_rules, with their promises
   */
  static void ConstructValidRules(GroupExpression *group_expr,
                                  OptimizeContext *context,
                                  std::vector<Rule*> &rules,
                                  std::vector<RuleWithPromise> &valid_rules);

  /**
   * Function to execute the task
   */
  virtual void execute() = 0;

  /**
   * @returns Memo used
   */
  inline Memo &GetMemo() const;

  /**
   * @returns RuleSet used
   */
  inline RuleSet &GetRuleSet() const;

  /**
   * Convenience to push a task onto same task pool
   * @param task Task to push
   */
  void PushTask(OptimizerTask* task);

  /**
   * Trivial destructor
   */
  virtual ~OptimizerTask() = default;

 protected:
  /**
   * Type of the OptimizerTask
   */
  OptimizerTaskType type_;

  /**
   * Current optimize context
   */
  OptimizeContext* context_;
};

/**
 * OptimizeGroup optimize a group within a given context.
 * OptimizeGroup will (1) generate all logically equivalent operator trees
 * if not already explored and (2) cost all physical operator trees given
 * the current OptimizeContext.
 */
class OptimizeGroup : public OptimizerTask {
 public:
  /**
   * Constructor for OptimizeGroup
   * @param group Group to optimize
   * @param context Current optimize context
   */
  OptimizeGroup(Group *group, OptimizeContext* context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_GROUP),
        group_(group) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * Group to optimize
   */
  Group *group_;
};

/**
 * OptimizeExpression optimizes a GroupExpression by constructing all logical
 * and physical transformations and applying those rules. The rules are sorted
 * by promise and applied in that order so a physical transformation rule is
 * applied before a logical transformation rule.
 */
class OptimizeExpression : public OptimizerTask {
 public:
  /**
   * Constructor for OptimizeExpression
   * @param group_expr GroupExpression to optimize
   * @param context Current optimize context
   */
  OptimizeExpression(GroupExpression *group_expr,
                     OptimizeContext *context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_EXPR),
        group_expr_(group_expr) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupExpression to optimize
   */
  GroupExpression *group_expr_;
};

/**
 * ExploreGroup generates all logical transformation expressions by applying
 * logical transformation rules to logical operators until saturation.
 */
class ExploreGroup : public OptimizerTask {
 public:
  /**
   * Constructor for ExploreGroup
   * @param group Group to explore
   * @param context Current optimize context
   */
  ExploreGroup(Group *group, OptimizeContext* context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_GROUP),
        group_(group) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * Group to explore
   */
  Group *group_;
};

/**
 * ExploreExpression applies logical transformation rules to a GroupExpression
 * until no more logical transformation rules can be applied. ExploreExpression
 * will also descend and explorenon-leaf children groups.
 */
class ExploreExpression : public OptimizerTask {
 public:
  /**
   * Constructor for ExploreExpression
   * @param group_expr GroupExpression to explore
   * @param context Current optimize context
   */
  ExploreExpression(GroupExpression *group_expr,
                    OptimizeContext* context)
      : OptimizerTask(context, OptimizerTaskType::EXPLORE_EXPR),
        group_expr_(group_expr) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupExpression to explore
   */
  GroupExpression *group_expr_;
};

/**
 * ApplyRule applies a rule. If it is a logical transformation rule, we need to
 * explore (apply logical rules) or optimize (apply logical & physical rules)
 * the new group expression based on the explore flag. If the rule is a
 * physical implementation rule, we directly cost the physical expression
 */
class ApplyRule : public OptimizerTask {
 public:
  /**
   * Constructor for ApplyRule
   * @param group_expr GroupExpression to apply the rule against
   * @param rule Rule to apply
   * @param context Current optimize context
   * @param explore Flag indicating whether explore or optimize
   */
  ApplyRule(GroupExpression *group_expr, Rule *rule,
            OptimizeContext* context,
            bool explore = false)
      : OptimizerTask(context, OptimizerTaskType::APPLY_RULE),
        group_expr_(group_expr),
        rule_(rule),
        explore_only(explore) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupExpression to apply rule against
   */
  GroupExpression *group_expr_;

  /**
   * Rule to apply
   */
  Rule *rule_;

  /**
   * Whether explore-only or explore and optimize
   */
  bool explore_only;
};

/**
 * OptimizeInputs costs a physical expression. The root operator is costed first
 * and the lowest cost of each child group is added. Finally, properties are
 * enforced to meet requirement in the context. We apply pruning by terminating if
 * the current expression's cost is larger than the upper bound of the current group
 */
class OptimizeInputs : public OptimizerTask {
 public:
  /**
   * Constructor for OptimizeInputs
   * @param group_expr GroupExpression to cost/optimize
   * @param context Current OptimizeContext
   */
  OptimizeInputs(GroupExpression *group_expr,
                 OptimizeContext* context)
      : OptimizerTask(context, OptimizerTaskType::OPTIMIZE_INPUTS),
        group_expr_(group_expr) {}

  /**
   * Constructor for OptimizeInputs
   * @param task OptimizeInputs task to construct from
   */
  explicit OptimizeInputs(OptimizeInputs *task)
      : OptimizerTask(task->context_, OptimizerTaskType::OPTIMIZE_INPUTS),
        output_input_properties_(std::move(task->output_input_properties_)),
        group_expr_(task->group_expr_),
        cur_total_cost_(task->cur_total_cost_),
        cur_child_idx_(task->cur_child_idx_),
        cur_prop_pair_idx_(task->cur_prop_pair_idx_) {}

  /**
   * Function to execute the task
   */
  void execute() override;

  /**
   * Destructor
   */
  ~OptimizeInputs() {
    for (auto &pair : output_input_properties_) {
      delete pair.first;
      for (auto &prop : pair.second) {
        delete prop;
      }
    }
  }

 private:
  /**
   * Vector of pairs of GroupExpression's output properties and input properties for children
   */
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>> output_input_properties_;

  /**
   * GroupExpression to optimize
   */
  GroupExpression *group_expr_;

  /**
   * Current total cost
   */
  double cur_total_cost_;

  /**
   * Current stage of enumeration through child groups
   */
  int cur_child_idx_ = -1;

  /**
   * Indicator of last child group that we waited for optimization
   */
  int prev_child_idx_ = -1;

  /**
   * Current stage of enumeration through output_input_properties_
   */
  int cur_prop_pair_idx_ = 0;
};

/**
 * DeriveStats derives any stats needed for costing a GroupExpression. This will
 * recursively derive stats and lazily collect stats for column needed.
 */
class DeriveStats : public OptimizerTask {
 public:
  /**
   * Constructor for DeriveStats
   * @param gexpr GroupExpression to derive stats for
   * @param required_cols Required expressions
   * @param context Current OptimizeContext
   */
  DeriveStats(GroupExpression *gexpr,
              ExprSet required_cols,
              OptimizeContext* context)
      : OptimizerTask(context, OptimizerTaskType::DERIVE_STATS),
        gexpr_(gexpr),
        required_cols_(required_cols) {}

  /**
   * Constructor for DeriveStats
   * @param task DeriveStats task to construct from
   */
  explicit DeriveStats(DeriveStats *task)
      : OptimizerTask(task->context_, OptimizerTaskType::DERIVE_STATS),
        gexpr_(task->gexpr_),
        required_cols_(task->required_cols_) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupExpression to derive stats for
   */
  GroupExpression *gexpr_;

  /**
   * Required columns
   */
  ExprSet required_cols_;
};

/**
 * TopDownRewrite performs a top-down rewrite pass. The task accepts a RuleSet
 * which requires that lower-level rewrites in the operator tree will not enable
 * upper-level rewrites i.e predicate push-down from upper to lower).
 */
class TopDownRewrite : public OptimizerTask {
 public:
  /**
   * Constructor for TopDownRewrite task
   * @param group_id Group to perform rewriting against
   * @param context Current optimize context
   * @param rule_set_name RuleSet to execute
   */
  TopDownRewrite(GroupID group_id, OptimizeContext* context,
                 RewriteRuleSetName rule_set_name)
      : OptimizerTask(context, OptimizerTaskType::TOP_DOWN_REWRITE),
        group_id_(group_id),
        rule_set_name_(rule_set_name) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupID to do top-down rewriting for
   */
  GroupID group_id_;

  /**
   * Set of rules to apply
   */
  RewriteRuleSetName rule_set_name_;
};

/**
 * BottomUpRewrite performs a bottom-up rewrite pass. The task accepts a RuleSet
 * which requires that upper level rewrites in the operator tree WILL NOT enable
 * lower level rewrites.
 */
class BottomUpRewrite : public OptimizerTask {
 public:
  /**
   * Constructor for BottomUpRewrite task
   * @param group_id Group to perform rewriting against
   * @param context Current optimize context
   * @param rule_set_name RuleSet to execute
   * @param has_optimized_child Flag indicating whether children have been optimized
   */
  BottomUpRewrite(GroupID group_id, OptimizeContext* context,
                  RewriteRuleSetName rule_set_name, bool has_optimized_child)
      : OptimizerTask(context, OptimizerTaskType::BOTTOM_UP_REWRITE),
        group_id_(group_id),
        rule_set_name_(rule_set_name),
        has_optimized_child_(has_optimized_child) {}

  /**
   * Function to execute the task
   */
  void execute() override;

 private:
  /**
   * GroupID to do bottom-up rewriting for
   */
  GroupID group_id_;

  /**
   * Set of rules to apply
   */
  RewriteRuleSetName rule_set_name_;

  /**
   * Flag indicating whether children have been optimized
   */
  bool has_optimized_child_;
};

}  // namespace optimizer
}  // namespace terrier
