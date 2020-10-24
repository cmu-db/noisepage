#include "planner/plannodes/csv_scan_plan_node.h"

#include <memory>
#include <string>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t CSVScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Filename
  hash = common::HashUtil::CombineHashes(hash, std::hash<std::string>{}(file_name_));

  // Delimiter
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delimiter_));

  // Quote Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(quote_));

  // Escape Char
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(escape_));

  hash = common::HashUtil::CombineHashInRange(hash, value_types_.begin(), value_types_.end());
  return hash;
}

// TODO(Gus): Are file names case sensitive?
bool CSVScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const CSVScanPlanNode &>(rhs);

  // Filename
  if (file_name_ != other.file_name_) return false;

  // Delimiter
  if (delimiter_ != other.delimiter_) return false;

  // Quote Char
  if (quote_ != other.quote_) return false;

  // Escape Char
  if (escape_ != other.escape_) return false;

  return value_types_ == other.value_types_;
}

nlohmann::json CSVScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["file_name"] = file_name_;
  j["delimiter"] = delimiter_;
  j["quote"] = quote_;
  j["escape"] = escape_;
  j["value_types"] = value_types_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CSVScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractScanPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  file_name_ = j.at("file_name").get<std::string>();
  delimiter_ = j.at("delimiter").get<char>();
  quote_ = j.at("quote").get<char>();
  escape_ = j.at("escape").get<char>();
  value_types_ = j.at("value_types").get<std::vector<type::TypeId>>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(CSVScanPlanNode);

}  // namespace noisepage::planner
