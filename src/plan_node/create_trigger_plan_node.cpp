#include "plan_node/create_trigger_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"

namespace terrier::plan_node {
common::hash_t CreateTriggerPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash trigger_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_name_));

  // Hash trigger_funcnames
  hash = common::HashUtil::CombineHashInRange(hash, trigger_funcnames_.begin(), trigger_funcnames_.end());

  // Hash trigger_args
  hash = common::HashUtil::CombineHashInRange(hash, trigger_args_.begin(), trigger_args_.end());

  // Hash trigger_columns
  hash = common::HashUtil::CombineHashInRange(hash, trigger_columns_.begin(), trigger_columns_.end());

  // Hash trigger_when
  hash = common::HashUtil::CombineHashes(hash, trigger_when_->Hash());

  // Hash trigger_type
  auto trigger_type = GetTriggerType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&trigger_type));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateTriggerPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateTriggerPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Hash trigger_name
  if (GetTriggerName() != other.GetTriggerName()) return false;

  // Trigger funcnames
  const auto &trigger_funcnames = GetTriggerFuncName();
  const auto &other_trigger_funcnames = other.GetTriggerFuncName();
  if (trigger_funcnames.size() != other_trigger_funcnames.size()) return false;

  for (size_t i = 0; i < trigger_funcnames.size(); i++) {
    if (trigger_funcnames[i] != other_trigger_funcnames[i]) {
      return false;
    }
  }

  // Trigger args
  const auto &trigger_args = GetTriggerArgs();
  const auto &other_trigger_args = other.GetTriggerArgs();
  if (trigger_args.size() != other_trigger_args.size()) return false;

  for (size_t i = 0; i < trigger_args.size(); i++) {
    if (trigger_args[i] != other_trigger_args[i]) {
      return false;
    }
  }

  // Trigger columns
  const auto &trigger_columns = GetTriggerColumns();
  const auto &other_trigger_columns = other.GetTriggerColumns();
  if (trigger_columns.size() != other_trigger_columns.size()) return false;

  for (size_t i = 0; i < trigger_columns.size(); i++) {
    if (trigger_columns[i] != other_trigger_columns[i]) {
      return false;
    }
  }

  // Hash trigger_when
  auto trigger_when = GetTriggerWhen();
  auto other_trigger_when = other.GetTriggerWhen();
  if ((trigger_when == nullptr && other_trigger_when != nullptr) ||
      (trigger_when != nullptr && other_trigger_when == nullptr))
    return false;
  if (trigger_when != nullptr && *trigger_when != *other_trigger_when) return false;

  // Hash trigger_type
  if (GetTriggerType() != other.GetTriggerType()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
