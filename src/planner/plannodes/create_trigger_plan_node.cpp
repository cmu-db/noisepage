#include "planner/plannodes/create_trigger_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"

namespace terrier::planner {
common::hash_t CreateTriggerPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash namespace oid
  auto namespace_oid = GetNamespaceOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid));

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

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

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

nlohmann::json CreateTriggerPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["trigger_name"] = trigger_name_;
  j["trigger_funcnames"] = trigger_funcnames_;
  j["trigger_args"] = trigger_args_;
  j["trigger_columns"] = trigger_columns_;
  j["trigger_when"] = trigger_when_;
  j["trigger_type"] = trigger_type_;
  return j;
}

void CreateTriggerPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  trigger_name_ = j.at("trigger_name").get<std::string>();
  trigger_funcnames_ = j.at("trigger_funcnames").get<std::vector<std::string>>();
  trigger_args_ = j.at("trigger_args").get<std::vector<std::string>>();
  trigger_columns_ = j.at("trigger_columns").get<std::vector<catalog::col_oid_t>>();

  if (!j.at("trigger_when").is_null()) {
    trigger_when_ = parser::DeserializeExpression(j.at("trigger_when"));
  }

  trigger_type_ = j.at("trigger_type").get<int16_t>();
}

}  // namespace terrier::planner
