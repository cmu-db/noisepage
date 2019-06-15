#pragma once

#include <map>
#include <unordered_set>
#include <vector>

#include "optimizer/operator_expression.h"
#include "optimizer/group.h"
#include "optimizer/group_expression.h"

namespace terrier {
namespace optimizer {

struct GExprPtrHash {
  std::size_t operator()(GroupExpression* const& s) const { return s->Hash(); }
};

struct GExprPtrEq {
  bool operator()(GroupExpression* const& t1, GroupExpression* const& t2) const {
    return *t1 == *t2;
  }
};

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
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
  GroupExpression* InsertExpression(GroupExpression* gexpr, bool enforced) {
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
  GroupExpression* InsertExpression(GroupExpression *gexpr, GroupID target_group, bool enforced);

  /**
   * Gets the group with certain ID
   * @param id ID of the group to get
   * @returns Group with specified ID
   */
  Group* GetGroupByID(GroupID id) const {
    return groups_[id];
  }

  /**
   * When a rewrite rule is applied, first replace original gexpr
   * with a new one. Convenience function to remove original gexpr.
   *
   * @param group_id GroupID of Group to erase
   */
  void EraseExpression(GroupID group_id) {
    auto gexpr = groups_[group_id]->GetLogicalExpression();
    group_expressions_.erase(gexpr);
    groups_[group_id]->EraseLogicalExpression();
  }

 private:
  /**
   * Creates a new group
   * @param gexpr GroupExpression to collect metadata from
   * @returns GroupID of the new group
   */
  GroupID AddNewGroup(GroupExpression* gexpr);

  // The group owns the group expressions, not the memo
  std::unordered_set<GroupExpression*, GExprPtrHash, GExprPtrEq> group_expressions_;
  std::vector<Group*> groups_;
};

}  // namespace optimizer
}  // namespace terrier
