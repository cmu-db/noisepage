#include "planner/plannodes/export_external_file_plan_node.h"
#include <memory>
#include <string>

namespace terrier::planner {

common::hash_t ExportExternalFilePlanNode::Hash() const {
  common::hash_t hash = std::hash<std::string>{}(file_name_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&escape_));
  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ExportExternalFilePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::EXPORT_EXTERNAL_FILE) return false;
  const auto &other = static_cast<const ExportExternalFilePlanNode &>(rhs);
  return (file_name_ == other.file_name_ && delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
          escape_ == other.escape_ && AbstractPlanNode::operator==(rhs));
}

nlohmann::json ExportExternalFilePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["file_name"] = file_name_;
  j["delimiter"] = delimiter_;
  j["quote"] = quote_;
  j["escape"] = escape_;
  return j;
}

void ExportExternalFilePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  file_name_ = j.at("file_name").get<std::string>();
  delimiter_ = j.at("delimiter").get<char>();
  quote_ = j.at("quote").get<char>();
  escape_ = j.at("escape").get<char>();
}
}  // namespace terrier::planner
