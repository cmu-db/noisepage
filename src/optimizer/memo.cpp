#include "optimizer/memo.h"

#include <string>
#include <unordered_set>
#include <utility>

#include "optimizer/group_expression.h"
#include "optimizer/logical_operators.h"

namespace noisepage::optimizer {

GroupExpression *Memo::InsertExpression(GroupExpression *gexpr, group_id_t target_group, bool enforced) {
  // If leaf, then just return
  if (gexpr->Contents()->GetOpType() == OpType::LEAF) {
    const auto leaf = gexpr->Contents()->GetContentsAs<LeafOperator>();
    NOISEPAGE_ASSERT(target_group == UNDEFINED_GROUP || target_group == leaf->GetOriginGroup(),
                     "target_group does not match the LeafOperator's group");
    gexpr->SetGroupID(leaf->GetOriginGroup());

    // Let the caller delete!
    // Caller needs the origin_group
    return nullptr;
  }

  // Lookup in hash table
  auto it = group_expressions_.find(gexpr);
  if (it != group_expressions_.end()) {
    NOISEPAGE_ASSERT(*gexpr == *(*it), "GroupExpression should be equal");
    delete gexpr;
    return *it;
  }

  group_expressions_.insert(gexpr);

  // New expression, so try to insert into an existing group or
  // create a new group if none specified
  group_id_t group_id;
  if (target_group == UNDEFINED_GROUP) {
    group_id = AddNewGroup(gexpr);
  } else {
    group_id = target_group;
  }

  Group *group = GetGroupByID(group_id);
  group->AddExpression(gexpr, enforced);
  return gexpr;
}

group_id_t Memo::AddNewGroup(GroupExpression *gexpr) {
  auto new_group_id = group_id_t(groups_.size());

  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;
  auto op_type = gexpr->Contents()->GetOpType();
  if (op_type == OpType::LOGICALGET) {
    // For base group, the table alias can get directly from logical get
    const auto logical_get = gexpr->Contents()->GetContentsAs<LogicalGet>();
    table_aliases.insert(logical_get->GetTableAlias());
  } else if (op_type == OpType::LOGICALQUERYDERIVEDGET) {
    const auto query_get = gexpr->Contents()->GetContentsAs<LogicalQueryDerivedGet>();
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

}  // namespace noisepage::optimizer
