#include "planner/plannodes/create_function_plan_node.h"

#include <memory>
#include <string>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<CreateFunctionPlanNode> CreateFunctionPlanNode::Builder::Build() {
  return std::unique_ptr<CreateFunctionPlanNode>(new CreateFunctionPlanNode(
      std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, language_,
      std::move(function_param_names_), std::move(function_param_types_), std::move(function_body_), is_replace_,
      std::move(function_name_), return_type_, param_count_, plan_node_id_));
}

CreateFunctionPlanNode::CreateFunctionPlanNode(
    std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, parser::PLType language,
    std::vector<std::string> &&function_param_names,
    std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
    std::vector<std::string> &&function_body, bool is_replace, std::string function_name,
    parser::BaseFunctionParameter::DataType return_type, int param_count, plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id),
      database_oid_(database_oid),
      namespace_oid_(namespace_oid),
      language_(language),
      function_param_names_(std::move(function_param_names)),
      function_param_types_(std::move(function_param_types)),
      function_body_(std::move(function_body)),
      is_replace_(is_replace),
      function_name_(std::move(function_name)),
      return_type_(return_type),
      param_count_(param_count) {}

common::hash_t CreateFunctionPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash language
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(language_));

  // Hash function_param_names
  hash = common::HashUtil::CombineHashInRange(hash, function_param_names_.begin(), function_param_names_.end());

  // Hash function_param_types
  hash = common::HashUtil::CombineHashInRange(hash, function_param_types_.begin(), function_param_types_.end());

  // Hash function_body
  hash = common::HashUtil::CombineHashInRange(hash, function_body_.begin(), function_body_.end());

  // Hash is_replace
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_replace_));

  // Hash function_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(function_name_));

  // Hash return_type
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(return_type_));

  // Hash param_count
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(param_count_));

  return hash;
}

bool CreateFunctionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateFunctionPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Language
  if (language_ != other.language_) return false;

  // Function param names
  if (function_param_names_ != other.function_param_names_) return false;

  // Function param types
  if (function_param_types_ != other.function_param_types_) return false;

  // Function body
  if (function_body_ != other.function_body_) return false;

  // Is replace
  if (is_replace_ != other.is_replace_) return false;

  // Function name
  if (function_name_ != other.function_name_) return false;

  // Return type
  if (return_type_ != other.return_type_) return false;

  // Param count
  if (param_count_ != other.param_count_) return false;

  return true;
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

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateFunctionPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
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
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(CreateFunctionPlanNode);

}  // namespace noisepage::planner
