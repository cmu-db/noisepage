#pragma once

#include <memory>
#include "optimizer/pattern.h"
#include "optimizer/optimize_context.h"
#include "optimizer/operator_expression.h"

namespace terrier {
namespace optimizer {

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
  INNER_JOIN_TO_NL_JOIN,
  INNER_JOIN_TO_HASH_JOIN,
  IMPLEMENT_DISTINCT,
  IMPLEMENT_LIMIT,
  EXPORT_EXTERNAL_FILE_TO_PHYSICAL,

  // Don't move this one
  RewriteDelimiter,

  // Rewrite rules (logical -> logical)
  PUSH_FILTER_THROUGH_JOIN,
  COMBINE_CONSECUTIVE_FILTER,
  EMBED_FILTER_INTO_GET,
  MARK_JOIN_GET_TO_INNER_JOIN,
  MARK_JOIN_INNER_JOIN_TO_INNER_JOIN,
  MARK_JOIN_FILTER_TO_INNER_JOIN,
  PULL_FILTER_THROUGH_MARK_JOIN,
  PULL_FILTER_THROUGH_AGGREGATION,

  // Place holder to generate number of rules compile time
  NUM_RULES
};

/**
 * Enum defining sets of rewrite rules
 */
enum class RewriteRuleSetName : uint32_t {
  PREDICATE_PUSH_DOWN = 0,
  UNNEST_SUBQUERY
};

/* Constant defining a physical rule's promise */
static const uint32_t PHYS_PROMISE = 3;

/* Constant defining a logical rule's promise */
static const uint32_t LOG_PROMISE = 1;

/**
 * The base class of all rules
 */
class Rule {
 public:
  /**
   * Destructor for Rule
   */
  virtual ~Rule() {
    delete match_pattern;
  }

  /**
   * Gets the match pattern for the rule
   * @returns match pattern
   */
  Pattern* GetMatchPattern() const { return match_pattern; }

  /**
   * Returns whether this rule is a physical rule or not
   * by checking LogicalPhysicalDelimiter and RewriteDelimiter.
   * @param whether the rule is a physical transformation
   */
  bool IsPhysical() const {
    return type_ > RuleType::LogicalPhysicalDelimiter && type_ < RuleType::RewriteDelimiter;
  }

  /**
   * Gets the type of the rule
   * @returns the type of the rule
   */
  inline RuleType GetType() { return type_; }

  /**
   * Gets the index of the rule w.r.t to bitmask
   * @returns index of the rule for bitmask
   */
  inline uint32_t GetRuleIdx() { return static_cast<uint32_t>(type_); }

  /**
   * Indicates whether the rule is a logical rule or not
   * @param whether the rule is a logical rule
   */
  bool IsLogical() const { return type_ < RuleType::LogicalPhysicalDelimiter; }

  /**
   * Indicates whether the rule is a rewrite rule or not
   * @param whether the rule is a rewrite rule
   */
  bool IsRewrite() const { return type_ > RuleType::RewriteDelimiter; }

  /**
   *  Get the promise of the current rule for a expression in the current
   *  context. Currently we only differentiate physical and logical rules.
   *  Physical rules have higher promise, and will be applied before logical
   *  rules. If the rule is not applicable because the pattern does not match,
   *  the promise should be 0, which indicates that we should not apply this
   *  rule
   *
   * @param group_expr The current group expression to apply the rule
   * @param context The current context for the optimization
   *
   * @return The promise, the higher the promise, the rule should be applied sooner
   */
  virtual int Promise(GroupExpression *group_expr, OptimizeContext *context) const;

  /**
   *  Check if the rule is applicable for the operator expression. The
   *  input operator expression should have the required "before" pattern, but
   *  other conditions may prevent us from applying the rule. For example, if
   *  the logical join does not specify a join key, we could not transform it
   *  into a hash join because we need the join key to build the hash table
   *
   *  @param expr The "before" operator expression
   *  @param context The current context for the optimization
   *  @return If the rule is applicable, return true, otherwise return false
   */
  virtual bool Check(OperatorExpression *expr, OptimizeContext *context) const = 0;

  /**
   * Convert a "before" operator tree to an "after" operator tree
   *
   * @param input The "before" operator tree
   * @param transformed Vector of "after" operator trees
   * @param context The current optimization context
   */
  virtual void Transform(
      OperatorExpression *input,
      std::vector<OperatorExpression*> &transformed,
      OptimizeContext *context) const = 0;

 protected:
  Pattern *match_pattern;
  RuleType type_;
};

/**
 * A struct to store a rule together with its promise.
 * The struct does not own the rule.
 */
struct RuleWithPromise {
  /**
   * Constructor
   * @param rule Pointer to rule
   * @param promise Promise of the rule
   */
  RuleWithPromise(Rule *rule, int promise) : rule(rule), promise(promise) {}

  Rule *rule;
  int promise;

  /**
   * Checks whether this is less than another RuleWithPromise by comparing promise.
   * @param r Other RuleWithPromise to compare against
   * @returns TRUE if this < r
   */
  bool operator<(const RuleWithPromise &r) const { return promise < r.promise; }

  /**
   * Checks whether this is larger than another RuleWithPromise by comparing promise.
   * @param r Other RuleWithPromise to compare against
   * @returns TRUE if this > r
   */
  bool operator>(const RuleWithPromise &r) const { return promise > r.promise; }
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
    for (auto rule : transformation_rules_) { delete rule; }
    for (auto rule : implementation_rules_) { delete rule; }
    for (auto &it : rewrite_rules_map_) {
      for (auto rule : it.second) {
        delete rule;
      }
    }
  }

  /**
   * Adds a transformation rule to the RuleSet
   * @param rule Rule to add
   */
  inline void AddTransformationRule(Rule* rule) { transformation_rules_.push_back(rule); }

  /**
   * Adds an implementation rule to the RuleSet
   * @param rule Rule to add
   */
  inline void AddImplementationRule(Rule* rule) { implementation_rules_.push_back(rule); }

  /**
   * Adds a rewrite rule to the RuleSet
   * @param set Rewrie RuleSet to add the rule to
   * @param rule Rule to add
   */
  inline void AddRewriteRule(RewriteRuleSetName set, Rule* rule) {
    rewrite_rules_map_[static_cast<uint32_t>(set)].push_back(rule);
  }

  /**
   * Gets all stored transformation rules
   * @returns vector of transformation rules
   */
  std::vector<Rule*> &GetTransformationRules() {
    return transformation_rules_;
  }

  /**
   * Gets all stored implementation rules
   * @returns vector of implementation rules
   */
  std::vector<Rule*> &GetImplementationRules() {
    return implementation_rules_;
  }

  /**
   * Gets all stored rules in a given RuleSet
   * @param Rewrite RuleSet to fetch
   * @returns vector of rules in that rewrite ruleset group
   */
  std::vector<Rule*> &GetRewriteRulesByName(RewriteRuleSetName set) {
    return rewrite_rules_map_[static_cast<uint32_t>(set)];
  }

 private:
  std::vector<Rule*> transformation_rules_;
  std::vector<Rule*> implementation_rules_;
  std::unordered_map<uint32_t, std::vector<Rule*>> rewrite_rules_map_;
};

}  // namespace optimizer
}  // namespace terrier
