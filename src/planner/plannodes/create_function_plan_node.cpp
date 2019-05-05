#include "planner/plannodes/create_function_plan_node.h"
#include <string>
#include <vector>
#include "storage/data_table.h"

namespace terrier::planner {

common::hash_t CreateFunctionPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash namespace oid
  auto namespace_oid = GetNamespaceOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid));

  // Hash language
  auto language = GetUDFLanguage();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&language));

  // Hash function_param_names
  for (const auto &function_param_name : function_param_names_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_param_name));
  }

  // Hash function_param_types
  for (const auto &function_param_type : function_param_types_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_param_type));
  }

  // Hash function_body
  // Hash function_param_types
  for (const auto &function_body_comp : function_body_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_body_comp));
  }

  // Hash is_replace
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_replace_));

  // Hash function_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_name_));

  // Hash return_type
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&return_type_));

  // Hash param_count
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&param_count_));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateFunctionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateFunctionPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Language
  if (GetUDFLanguage() != other.GetUDFLanguage()) return false;

  // Function param names
  const auto &function_param_names = GetFunctionParameterNames();
  const auto &other_function_param_names = other.GetFunctionParameterNames();
  if (function_param_names.size() != other_function_param_names.size()) return false;

  for (size_t i = 0; i < function_param_names.size(); i++) {
    if (function_param_names[i] != other_function_param_names[i]) {
      return false;
    }
  }

  // Function param types
  const auto &function_param_types = GetFunctionParameterTypes();
  const auto &other_function_param_types = other.GetFunctionParameterTypes();
  if (function_param_types.size() != other_function_param_types.size()) return false;

  for (size_t i = 0; i < function_param_types.size(); i++) {
    if (function_param_types[i] != other_function_param_types[i]) {
      return false;
    }
  }

  // Function body
  const auto &function_body = GetFunctionBody();
  const auto &other_function_body = other.GetFunctionBody();
  if (function_body.size() != other_function_body.size()) return false;

  for (size_t i = 0; i < function_body.size(); i++) {
    if (function_body[i] != other_function_body[i]) {
      return false;
    }
  }

  // Is replace
  if (IsReplace() != other.IsReplace()) return false;

  // Function name
  if (GetFunctionName() != other.GetFunctionName()) return false;

  // Return type
  if (GetReturnType() != other.GetReturnType()) return false;

  // Param count
  if (GetParamCount() != other.GetParamCount()) return false;
  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json CreateFunctionPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["language"] = language_;
  j["function_param_names"] = function_param_names_;
  j["function_param_types"] = function_param_types_;
  j["function_body"] = function_body_;
  j["is_replace"] = is_replace_;
  j["function_name"] = function_name_;
  j["return_type"] = return_type_;
  j["param_count"] = param_count_;
  return j;
}

void CreateFunctionPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  language_ = j.at("language").get<parser::PLType>();
  function_param_names_ = j.at("function_param_names").get<std::vector<std::string>>();
  function_param_types_ = j.at("function_param_types").get<std::vector<parser::BaseFunctionParameter::DataType>>();
  function_body_ = j.at("function_body").get<std::vector<std::string>>();
  is_replace_ = j.at("is_replace").get<bool>();
  function_name_ = j.at("function_name").get<std::string>();
  return_type_ = j.at("return_type").get<parser::BaseFunctionParameter::DataType>();
  param_count_ = j.at("param_count").get<int>();
}

}  // namespace terrier::planner
