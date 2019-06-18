#include "optimizer/group_expression.h"
#include "optimizer/memo.h"
#include "optimizer/logical_operators.h"

namespace terrier {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
GroupExpression *Memo::InsertExpression(GroupExpression* gexpr, GroupID target_group, bool enforced) {
  // If leaf, then just return
  if (gexpr->Op().GetType() == OpType::LEAF) {
    const LeafOperator *leaf = gexpr->Op().As<LeafOperator>();
    TERRIER_ASSERT(target_group == UNDEFINED_GROUP || target_group == leaf->GetOriginGroup(),
                   "target_group does not match the LeafOperator's group");
    gexpr->SetGroupID(leaf->GetOriginGroup());

    // Let the caller delete!
    // Caller needs the origin_group
    return nullptr;
  }

  // Lookup in hash table
  auto it = group_expressions_.find(gexpr);
  if (it != group_expressions_.end()) {
    TERRIER_ASSERT(*gexpr == *(*it), "GroupExpression should be equal");
    delete gexpr;
    return *it;
  } else {
    group_expressions_.insert(gexpr);

    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupID group_id;
    if (target_group == UNDEFINED_GROUP) {
      group_id = AddNewGroup(gexpr);
    } else {
      group_id = target_group;
    }

    Group *group = GetGroupByID(group_id);
    group->AddExpression(gexpr, enforced);
    return gexpr;
  }
}

GroupID Memo::AddNewGroup(GroupExpression* gexpr) {
  GroupID new_group_id = static_cast<GroupID>(groups_.size());

  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;
  auto op_type = gexpr->Op().GetType();
  if (op_type == OpType::LOGICALGET) {
    // For base group, the table alias can get directly from logical get
    const LogicalGet *logical_get = gexpr->Op().As<LogicalGet>();
    table_aliases.insert(logical_get->GetTableAlias());
  } else if (op_type == OpType::LOGICALQUERYDERIVEDGET) {
    const LogicalQueryDerivedGet *query_get = gexpr->Op().As<LogicalQueryDerivedGet>();
    table_aliases.insert(query_get->GetTableAlias());
  } else {
    // For other groups, need to aggregate the table alias from children
    for (auto child_group_id : gexpr->GetChildGroupIDs()) {
      Group *child_group = GetGroupByID(child_group_id);
      for (auto &table_alias : child_group->GetTableAliases()) {
        table_aliases.insert(table_alias);
      }
    }
  }

  groups_.push_back(new Group(new_group_id, std::move(table_aliases)));
  return new_group_id;
}

}  // namespace optimizer
}  // namespace terrier
