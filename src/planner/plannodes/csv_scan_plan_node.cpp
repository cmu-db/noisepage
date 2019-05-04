#include "planner/plannodes/csv_scan_plan_node.h"
#include <memory>
#include <string>

namespace terrier::planner {

common::hash_t CSVScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, std::hash<std::string>{}(file_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&escape_));
  hash = common::HashUtil::CombineHashes(hash, std::hash<std::string>{}(null_string_));

  return hash;
}

// TODO(Gus): Are file names case sensitive?
bool CSVScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const CSVScanPlanNode &>(rhs);
  return file_name_ == other.file_name_ && delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
         escape_ == other.escape_ && null_string_ == other.null_string_;
}

nlohmann::json CSVScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["file_name"] = file_name_;
  j["delimiter"] = delimiter_;
  j["quote"] = quote_;
  j["escape"] = escape_;
  j["null_string"] = null_string_;
  return j;
}

void CSVScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractScanPlanNode::FromJson(j);
  file_name_ = j.at("file_name").get<std::string>();
  delimiter_ = j.at("delimiter").get<char>();
  quote_ = j.at("quote").get<char>();
  escape_ = j.at("escape").get<char>();
  null_string_ = j.at("null_string").get<std::string>();
}

}  // namespace terrier::planner
