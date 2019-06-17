#include "planner/plannodes/export_external_file_plan_node.h"
#include <memory>
#include <string>

namespace terrier::planner {

common::hash_t ExportExternalFilePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Filename
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(file_name_));

  // Delimiter
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));

  // Quote Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));

  // Escape Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));

  return hash;
}

bool ExportExternalFilePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const ExportExternalFilePlanNode &>(rhs);

  // Filename
  if (file_name_ != other.file_name_) return false;

  // Delimiter
  if (delimiter_ != other.delimiter_) return false;

  // Quote Char
  if (quote_ != other.quote_) return false;

  // Escape Char
  if (escape_ != other.escape_) return false;

  return true;
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
