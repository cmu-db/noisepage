#pragma once

#include <map>
#include <unordered_set>
#include <vector>

#include "optimizer/group.h"
#include "optimizer/group_expression.h"
#include "optimizer/operator_node.h"

namespace noisepage::optimizer {

/**
 * Struct implementing the Hash() function for a GroupExpression*
 */
struct GExprPtrHash {
  /**
   * Implements the hash() function for GroupExpression
   * @param s GroupExpression to hash
   * @returns hash code
   */
  std::size_t operator()(GroupExpression *const &s) const {
    if (s == nullptr) return 0;
    return s->Hash();
  }
};

/**
 * Struct implementing the == function for GroupExpression*
 */
struct GExprPtrEq {
  /**
   * Implements the == operator for GroupExpression
   * @param t1 One of the inputs GroupExpression
   * @param t2 GroupExpression to check equality with against t1
   * @returns TRUE if equal
   */
  bool operator()(GroupExpression *const &t1, GroupExpression *const &t2) const { return (*t1 == *t2); }
};

/**
 * Memo class provides for tracking Groups and GroupExpressions and provides the
 * mechanisms by which we can do duplicate group detection.
 */
class Memo {
 public:
  /**
   * Constructor
   */
  Memo() = default;

  /**
   * Destructor
   */
  ~Memo() {
    for (auto group : groups_) {
      delete group;
    }
  }

  /**
   * Adds a group expression into the proper group in the memo,
   * checking for duplicates.
   *
   * @note
   * gexpr's lifetime is controlled by memo/group after invocation if
   * this function returns a non-null pointer.
   *
   * @param gexpr GroupExpression to insert
   * @param enforced if the new expression is created by enforcer
   * @returns Existing expression if found. Otherwise, return the new expr.
   *          Returns nullptr if the gexpr is a placeholder (Wildcard)
   */
  GroupExpression *InsertExpression(GroupExpression *gexpr, bool enforced) {
    return InsertExpression(gexpr, UNDEFINED_GROUP, enforced);
  }

  /**
   * Adds a group expression into the proper group in the memo,
   * checking for duplicates.
   *
   * @note
   * gexpr's lifetime is controlled by memo/group after invocation if
   * this function returns a non-null pointer.
   *
   * @param gexpr GroupExpression to insert
   * @param target_group Group to insert into
   * @param enforced if the new expression is created by enforcer
   * @returns Existing expression if found. Otherwise, return the new expr
   *          Returns nullptr if the gexpr is a placeholder (Wildcard)
   */
  GroupExpression *InsertExpression(GroupExpression *gexpr, group_id_t target_group, bool enforced);

  /**
   * Gets the group with certain ID
   * @param id ID of the group to get
   * @returns Group with specified ID
   */
  Group *GetGroupByID(group_id_t id) const {
    auto idx = id.UnderlyingValue();
    NOISEPAGE_ASSERT(idx >= 0 && static_cast<size_t>(idx) < groups_.size(), "group_id out of bounds");
    return groups_[idx];
  }

  /**
   * When a rewrite rule is applied, first replace original gexpr
   * with a new one. Convenience function to remove original gexpr.
   *
   * @param group_id GroupID of Group to erase
   */
  void EraseExpression(group_id_t group_id) {
    auto idx = group_id.UnderlyingValue();
    NOISEPAGE_ASSERT(idx >= 0 && static_cast<size_t>(idx) < groups_.size(), "group_id out of bounds");

    auto gexpr = groups_[idx]->GetLogicalExpression();
    group_expressions_.erase(gexpr);
    groups_[idx]->EraseLogicalExpression();
  }

 private:
  /**
   * Creates a new group
   * @param gexpr GroupExpression to collect metadata from
   * @returns GroupID of the new group
   */
  group_id_t AddNewGroup(GroupExpression *gexpr);

  /**
   * Vector of tracked GroupExpressions
   * Group owns GroupExpressions, not the memo
   */
  std::unordered_set<GroupExpression *, GExprPtrHash, GExprPtrEq> group_expressions_;

  /**
   * Vector of groups tracked
   */
  std::vector<Group *> groups_;
};

}  // namespace noisepage::optimizer
