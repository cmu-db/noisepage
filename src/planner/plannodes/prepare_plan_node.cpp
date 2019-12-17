#include "planner/plannodes/prepare_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::planner {

common::hash_t PreparePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(name_));

  // dml_statement
  if (dml_statement_ != nullptr) hash = common::HashUtil::CombineHashes(hash, dml_statement_->Hash());

  // parameters
  if (parameters_ != nullptr) {
    for (const auto &parameter : *parameters_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(parameter->Hash()));
    }
  }
  return hash;
}

bool PreparePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const PreparePlanNode &>(rhs);

  // name
  if (name_ != other.name_) return false;

  // dml_statement
  if (dml_statement_ == nullptr && other.dml_statement_ != nullptr) return false;
  if (dml_statement_ != nullptr && other.dml_statement_ == nullptr) return false;
  if (dml_statement_ != nullptr && other.dml_statement_ != nullptr) {
    if (*dml_statement_ != *other.dml_statement_) return false;
  }

  // parameters
  if (parameters_ == nullptr && other.parameters_ != nullptr) return false;
  if (parameters_ != nullptr && other.parameters_ == nullptr) return false;
  if (parameters_ != nullptr && other.parameters_ != nullptr) {
    for (size_t i = 0; i < parameters_->size(); i++) {
      if (*((*parameters_)[i]) != *((*other.parameters_)[i])) return false;
    }
  }
  return true;
}

nlohmann::json PreparePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["name"] = name_;
  j["dml_statement"] = dml_statement_ == nullptr ? nlohmann::json(nullptr) : dml_statement_->ToJson();

  std::vector<nlohmann::json> parameters_json;
  parameters_json.reserve(parameters_->size());
  for (const auto &parameter : *parameters_) {
    parameters_json.emplace_back(parameter->ToJson());
  }
  j["parameters"] = parameters_json;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> PreparePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  name_ = j.at("name").get<std::string>();
  return exprs;
}

}  // namespace terrier::planner
