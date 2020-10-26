#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "optimizer/operator_node.h"
#include "optimizer/optimization_context.h"
#include "optimizer/pattern.h"

namespace noisepage::optimizer {

/**
 * Enum defining the types of rules
 */
enum class RuleType : uint32_t {
  // Transformation rules (logical -> logical)
  INNER_JOIN_COMMUTE = 0,
  INNER_JOIN_ASSOCIATE,

  // Don't move this one
  LogicalPhysicalDelimiter,

  // Implementation rules (logical -> physical)
  GET_TO_DUMMY_SCAN,
  GET_TO_SEQ_SCAN,
  GET_TO_INDEX_SCAN,
  QUERY_DERIVED_GET_TO_PHYSICAL,
  EXTERNAL_FILE_GET_TO_PHYSICAL,
  DELETE_TO_PHYSICAL,
  UPDATE_TO_PHYSICAL,
  INSERT_TO_PHYSICAL,
  INSERT_SELECT_TO_PHYSICAL,
  AGGREGATE_TO_HASH_AGGREGATE,
  AGGREGATE_TO_PLAIN_AGGREGATE,
  INNER_JOIN_TO_INDEX_JOIN,
  INNER_JOIN_TO_NL_JOIN,
  SEMI_JOIN_TO_HASH_JOIN,
  INNER_JOIN_TO_HASH_JOIN,
  LEFT_JOIN_TO_HASH_JOIN,
  IMPLEMENT_DISTINCT,
  IMPLEMENT_LIMIT,
  EXPORT_EXTERNAL_FILE_TO_PHYSICAL,
  ANALYZE_TO_PHYSICAL,

  // Create/Drop
  CREATE_DATABASE_TO_PHYSICAL,
  CREATE_FUNCTION_TO_PHYSICAL,
  CREATE_INDEX_TO_PHYSICAL,
  CREATE_TABLE_TO_PHYSICAL,
  CREATE_NAMESPACE_TO_PHYSICAL,
  CREATE_TRIGGER_TO_PHYSICAL,
  CREATE_VIEW_TO_PHYSICAL,
  DROP_DATABASE_TO_PHYSICAL,
  DROP_TABLE_TO_PHYSICAL,
  DROP_INDEX_TO_PHYSICAL,
  DROP_NAMESPACE_TO_PHYSICAL,
  DROP_TRIGGER_TO_PHYSICAL,
  DROP_VIEW_TO_PHYSICAL,

  // Don't move this one
  RewriteDelimiter,

  // Rewrite rules (logical -> logical)
  PUSH_FILTER_THROUGH_JOIN,
  PUSH_FILTER_THROUGH_AGGREGATION,
  COMBINE_CONSECUTIVE_FILTER,
  EMBED_FILTER_INTO_GET,
  MARK_JOIN_GET_TO_INNER_JOIN,
  SINGLE_JOIN_GET_TO_INNER_JOIN,
  DEPENDENT_JOIN_GET_TO_INNER_JOIN,
  MARK_JOIN_INNER_JOIN_TO_INNER_JOIN,
  MARK_JOIN_FILTER_TO_INNER_JOIN,
  PULL_FILTER_THROUGH_MARK_JOIN,
  PULL_FILTER_THROUGH_AGGREGATION,

  // Place holder to generate number of rules compile time
  NUM_RULES
};

/**
 * Enum defining set of rules
 */
enum class RuleSetName : uint32_t {
  PREDICATE_PUSH_DOWN = 0,
  UNNEST_SUBQUERY,
  LOGICAL_TRANSFORMATION,
  PHYSICAL_IMPLEMENTATION
};

/**
 * Enum defining rule promises
 * LogicalPromise should be used for logical rules and/or low priority unnest
 */
enum class RulePromise : uint32_t {
  /**
   * NO promise. Rule cannot be used
   */
  NO_PROMISE = 0,

  /**
   * Logical rule/low priority unnest
   */
  LOGICAL_PROMISE = 1,

  /**
   * High priority unnest
   */
  UNNEST_PROMISE_HIGH = 2,

  /**
   * Physical rule
   */
  PHYSICAL_PROMISE = 3
};

/**
 * The base class of all rules
 */
class Rule {
 public:
  /**
   * Destructor for Rule
   */
  virtual ~Rule() { delete match_pattern_; }

  /**
   * Gets the match pattern for the rule
   * @returns match pattern
   */
  Pattern *GetMatchPattern() const { return match_pattern_; }

  /**
   * Returns whether this rule is a physical rule or not
   * by checking LogicalPhysicalDelimiter and RewriteDelimiter.
   * @returns whether the rule is a physical transformation
   */
  bool IsPhysical() const { return type_ > RuleType::LogicalPhysicalDelimiter && type_ < RuleType::RewriteDelimiter; }

  /**
   * Gets the type of the rule
   * @returns the type of the rule
   */
  RuleType GetType() { return type_; }

  /**
   * Gets the index of the rule w.r.t to bitmask
   * @returns index of the rule for bitmask
   */
  uint32_t GetRuleIdx() { return static_cast<uint32_t>(type_); }

  /**
   * Indicates whether the rule is a logical rule or not
   * @returns whether the rule is a logical rule
   */
  bool IsLogical() const { return type_ < RuleType::LogicalPhysicalDelimiter; }

  /**
   * Indicates whether the rule is a rewrite rule or not
   * @returns whether the rule is a rewrite rule
   */
  bool IsRewrite() const { return type_ > RuleType::RewriteDelimiter; }

  /**
   * Get the promise of the current rule for a expression in the current
   * context. Currently we only differentiate physical and logical rules.
   * Physical rules have higher promise, and will be applied before logical
   * rules. If the rule is not applicable because the pattern does not match,
   * the promise should be 0, which indicates that we should not apply this
   * rule.
   *
   * @param group_expr The current group expression to apply the rule
   *
   * @return The promise, the higher the promise, the rule should be applied sooner
   */
  virtual RulePromise Promise(GroupExpression *group_expr) const;

  /**
   * Check if the rule is applicable for the operator expression. The
   * input operator expression should have the required "before" pattern, but
   * other conditions may prevent us from applying the rule. For example, if
   * the logical join does not specify a join key, we could not transform it
   * into a hash join because we need the join key to build the hash table
   *
   * @param expr The "before" operator expression
   * @param context The current context for the optimization
   * @return If the rule is applicable, return true, otherwise return false
   */
  virtual bool Check(common::ManagedPointer<AbstractOptimizerNode> expr, OptimizationContext *context) const = 0;

  /**
   * Convert a "before" operator tree to an "after" operator tree
   *
   * @param input The "before" operator tree
   * @param transformed Vector of "after" operator trees
   * @param context The current optimization context
   */
  virtual void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                         std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                         OptimizationContext *context) const = 0;

 protected:
  /**
   * Match pattern for the rule to be used
   */
  Pattern *match_pattern_;

  /**
   * Type of the rule
   */
  RuleType type_;
};

/**
 * A struct to store a rule together with its promise.
 * The struct does not own the rule.
 */
struct RuleWithPromise {
 public:
  /**
   * Constructor
   * @param rule Pointer to rule
   * @param promise Promise of the rule
   */
  RuleWithPromise(Rule *rule, RulePromise promise) : rule_(rule), promise_(promise) {}

  /**
   * Gets the rule
   * @returns Rule
   */
  Rule *GetRule() { return rule_; }

  /**
   * Gets the promise
   * @returns Promise
   */
  RulePromise GetPromise() { return promise_; }

  /**
   * Checks whether this is less than another RuleWithPromise by comparing promise.
   * @param r Other RuleWithPromise to compare against
   * @returns TRUE if this < r
   */
  bool operator<(const RuleWithPromise &r) const { return promise_ < r.promise_; }

  /**
   * Checks whether this is larger than another RuleWithPromise by comparing promise.
   * @param r Other RuleWithPromise to compare against
   * @returns TRUE if this > r
   */
  bool operator>(const RuleWithPromise &r) const { return promise_ > r.promise_; }

 private:
  /**
   * Rule
   */
  Rule *rule_;

  /**
   * Promise
   */
  RulePromise promise_;
};

/**
 * Defines a RuleSet, containing a collection of rules.
 * A RuleSet "owns" the rules.
 */
class RuleSet {
 public:
  /**
   * Default constructor
   */
  RuleSet();

  /**
   * Destructor
   */
  ~RuleSet() {
    for (auto &it : rules_map_) {
      for (auto rule : it.second) {
        delete rule;
      }
    }
  }

  /**
   * Adds a rule to the RuleSet
   * @param set RuleSet to add the rule to
   * @param rule Rule to add
   */
  void AddRule(RuleSetName set, Rule *rule) { rules_map_[static_cast<uint32_t>(set)].push_back(rule); }

  /**
   * Gets all stored rules in a given RuleSet
   * @param set Rewrite RuleSet to fetch
   * @returns vector of rules in that rewrite ruleset group
   */
  std::vector<Rule *> &GetRulesByName(RuleSetName set) { return rules_map_[static_cast<uint32_t>(set)]; }

 private:
  /**
   * Map from RuleSetName (uint32_t) -> vector of rules
   */
  std::unordered_map<uint32_t, std::vector<Rule *>> rules_map_;
};

}  // namespace noisepage::optimizer
