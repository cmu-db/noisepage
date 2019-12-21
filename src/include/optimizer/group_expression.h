#pragma once

#include <bitset>
#include <map>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "optimizer/group.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/property_set.h"
#include "optimizer/rule.h"

namespace terrier::optimizer {

/**
 * GroupExpression used to represent a particular logical or physical
 * operator expression within a group that abstracts away the specific
 * OperatorExpression of a Group.
 */
class GroupExpression {
 public:
  /**
   * Constructor for GroupExpression
   * @param op Operator
   * @param child_groups Vector of children groups
   */
  GroupExpression(Operator op, std::vector<group_id_t> &&child_groups)
      : group_id_(UNDEFINED_GROUP), op_(std::move(op)), child_groups_(child_groups), stats_derived_(false) {}

  /**
   * Destructor. Deletes everything in the lowest_cost_table_
   * (both keys and values).
   */
  ~GroupExpression() {
    for (auto it : lowest_cost_table_) {
      delete it.first;

      std::vector<PropertySet *> &props = std::get<1>(it.second);
      for (auto prop : props) {
        delete prop;
      }
    }
  }

  /**
   * Gets the GroupExpression's GroupID
   * @returns GroupID of the group this expression belongs to
   */
  group_id_t GetGroupID() const { return group_id_; }

  /**
   * Sets this GroupExpression's GroupID
   * @param id GroupID of the expression
   */
  void SetGroupID(group_id_t id) { group_id_ = id; }

  /**
   * Gets the vector of child GroupIDs
   * @return child GroupIDs
   */
  const std::vector<group_id_t> &GetChildGroupIDs() const { return child_groups_; }

  /**
   * Gets a specific child GroupID
   * @param child_idx Index of the child
   * @returns Child's GroupID
   */
  group_id_t GetChildGroupId(int child_idx) const {
    TERRIER_ASSERT(child_idx >= 0 && static_cast<size_t>(child_idx) < child_groups_.size(),
                   "child_idx is out of bounds");
    return child_groups_[child_idx];
  }

  /**
   * Gets the operator wrapped by this GroupExpression
   * @returns Operator
   */
  const Operator &Op() const { return op_; }

  /**
   * Retrieves the lowest cost satisfying a given set of properties
   * @param requirements PropertySet that needs to be satisfied
   * @returns Lowest cost to satisfy that PropertySet
   */
  double GetCost(PropertySet *requirements) const { return std::get<0>(lowest_cost_table_.find(requirements)->second); }

  /**
   * Gets the input properties needed for a given required properties
   * @param requirements PropertySet that needs to be satisfied
   * @returns vector of children input properties required
   */
  std::vector<PropertySet *> GetInputProperties(PropertySet *requirements) const {
    return std::get<1>(lowest_cost_table_.find(requirements)->second);
  }

  /**
   * Appends to the internal hash table a mapping from output_properties to
   * a tuple of lowest cost and vector of children input properties.
   *
   * @note output_properties and input_properties_list ownership of PropertySet
   * is given to this GroupExpression! Once this function is called, the pointers
   * are no longer guaranteed to be valid....
   *
   * @param output_properties PropertySet that is satisfied
   * @param input_properties_list Vector of children input properties required
   * @param cost Cost
   */
  void SetLocalHashTable(PropertySet *output_properties, std::vector<PropertySet *> input_properties_list, double cost);

  /**
   * Hashes GroupExpression
   * @returns hash code of GroupExpression
   */
  common::hash_t Hash() const;

  /**
   * Checks for equality for GroupExpression
   * @param r Other GroupExpression
   * @returns TRUE if equal to other GroupExpression
   */
  bool operator==(const GroupExpression &r) { return (op_ == r.Op()) && (child_groups_ == r.child_groups_); }

  /**
   * Marks a rule as having being explored in this GroupExpression
   * @param rule Rule to mark as explored
   */
  void SetRuleExplored(Rule *rule) { rule_mask_.set(rule->GetRuleIdx(), true); }

  /**
   * Checks whether a rule has been explored
   * @param rule Rule to see if explored
   * @returns TRUE if the rule has been explored already
   */
  bool HasRuleExplored(Rule *rule) { return rule_mask_.test(rule->GetRuleIdx()); }

  /**
   * Sets a flag indicating stats have been derived
   */
  void SetDerivedStats() { stats_derived_ = true; }

  /**
   * @returns whether stats have been derived
   */
  bool HasDerivedStats() { return stats_derived_; }

  /**
   * Gets number of children groups
   * @returns Number of child groups
   */
  size_t GetChildrenGroupsSize() const { return child_groups_.size(); }

 private:
  /**
   * Group's ID
   */
  group_id_t group_id_;

  /**
   * Operator
   */
  Operator op_;

  /**
   * Vector of child groups
   */
  std::vector<group_id_t> child_groups_;

  /**
   * Mask of explored rules
   */
  std::bitset<static_cast<uint32_t>(RuleType::NUM_RULES)> rule_mask_;

  /**
   * Flag of whether stats are derived
   */
  bool stats_derived_;

  /**
   * Mapping from output properties to the corresponding best cost, statistics,
   * and child properties
   */
  std::unordered_map<PropertySet *, std::tuple<double, std::vector<PropertySet *>>, PropSetPtrHash, PropSetPtrEq>
      lowest_cost_table_;
};

}  // namespace terrier::optimizer

namespace std {

/**
 * Implementation of std::hash for GroupExpression
 */
template <>
struct hash<terrier::optimizer::GroupExpression> {
  /**
   * Defines argument_type to be GroupExpression
   */
  using argument_type = terrier::optimizer::GroupExpression;

  /**
   * Defines result_type to be size_t
   */
  using result_type = std::size_t;

  /**
   * Implementation of the hash() for GroupExpression
   * @param s GroupExpression to hash
   * @returns hash code
   */
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
