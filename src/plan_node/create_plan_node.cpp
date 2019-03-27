#include "plan_node/create_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"

namespace terrier::plan_node {
common::hash_t CreatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash create_type
  auto create_type = GetCreateType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&create_type));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetSchemaName()));

  // Hash database_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetDatabaseName()));

  // Hash index_type
  auto index_type = GetIndexType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&index_type));

  // Hash index_attrs
  hash = common::HashUtil::CombineHashInRange(hash, index_attrs_.begin(), index_attrs_.end());

  // Hash key_attrs
  hash = common::HashUtil::CombineHashInRange(hash, key_attrs_.begin(), key_attrs_.end());

  // Hash unique_index
  auto unique_index = IsUniqueIndex();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&unique_index));

  // Hash index_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetIndexName()));

  // Hash has primary_key
  auto has_primary_key = HasPrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&has_primary_key));

  // Hash primary_key
  hash = common::HashUtil::CombineHashes(hash, primary_key_.Hash());

  // Hash foreign_keys
  for (const auto &foreign_key : foreign_keys_) {
    hash = common::HashUtil::CombineHashes(hash, foreign_key.Hash());
  }

  // Hash con_uniques
  for (const auto &con_unique : con_uniques_) {
    hash = common::HashUtil::CombineHashes(hash, con_unique.Hash());
  }

  // Hash con_checks
  for (const auto &con_check : con_checks_) {
    hash = common::HashUtil::CombineHashes(hash, con_check.Hash());
  }

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
  hash =
      common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&trigger_type));

  // Hash view_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));

  // TODO(Gus,Wen) missing Hash for select statement

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreatePlanNode &>(rhs);

  // Create type
  if (GetCreateType() != other.GetCreateType()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Schema name
  if (GetSchemaName() != other.GetSchemaName()) return false;

  // Database name
  if (GetDatabaseName() != other.GetDatabaseName()) return false;

  // Index type
  if (GetIndexType() != other.GetIndexType()) return false;

  // Index attrs
  const auto &index_attrs = GetIndexAttributes();
  const auto &other_index_attrs = other.GetIndexAttributes();
  if (index_attrs.size() != other_index_attrs.size()) return false;

  for (size_t i = 0; i < index_attrs.size(); i++) {
    if (index_attrs[i] != other_index_attrs[i]) {
      return false;
    }
  }

  // Key attrs
  const auto &key_attrs = GetKeyAttrs();
  const auto &other_key_attrs = other.GetKeyAttrs();
  if (key_attrs.size() != other_key_attrs.size()) return false;

  for (size_t i = 0; i < key_attrs.size(); i++) {
    if (key_attrs[i] != other_key_attrs[i]) {
      return false;
    }
  }

  // Unique index
  if (IsUniqueIndex() != other.IsUniqueIndex()) return false;

  // Index name
  if (GetIndexName() != other.GetIndexName()) return false;

  // Has primary key
  if (HasPrimaryKey() != other.HasPrimaryKey()) return false;

  // Foreign key
  const auto &foreign_keys_ = GetForeignKeys();
  const auto &other_foreign_keys_ = other.GetForeignKeys();
  if (foreign_keys_.size() != other_foreign_keys_.size()) return false;

  for (size_t i = 0; i < foreign_keys_.size(); i++) {
    if (foreign_keys_[i] != other_foreign_keys_[i]) {
      return false;
    }
  }

  // Unique constraints
  const auto &con_uniques = GetUniqueConstraintss();
  const auto &other_con_uniques = other.GetUniqueConstraintss();
  if (con_uniques.size() != other_con_uniques.size()) return false;

  for (size_t i = 0; i < con_uniques.size(); i++) {
    if (con_uniques[i] != other_con_uniques[i]) {
      return false;
    }
  }

  // Check constraints
  const auto &con_checks = GetCheckConstrinats();
  const auto &other_con_check = other.GetCheckConstrinats();
  if (con_checks.size() != other_con_check.size()) return false;

  for (size_t i = 0; i < con_checks.size(); i++) {
    if (con_checks[i] != other_con_check[i]) {
      return false;
    }
  }

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

  // Hash view_name
  if (GetViewName() != other.GetViewName()) return false;

  // TODO(Gus,Wen) missing == operator for select statement

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
