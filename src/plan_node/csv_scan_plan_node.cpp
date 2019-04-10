#include "plan_node/csv_scan_plan_node.h"
#include <memory>
#include <string>

namespace terrier::plan_node {

common::hash_t CSVScanPlanNode::Hash() const {
  common::hash_t hash = std::hash<std::string>{}(file_name_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&escape_));
  hash = common::HashUtil::CombineHashes(hash, std::hash<std::string>{}(null_string_));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

// TODO(Gus): Are file names case sensitive?
bool CSVScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::CSVSCAN) return false;
  const auto &other = static_cast<const CSVScanPlanNode &>(rhs);
  return (file_name_ == other.file_name_ && delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
          escape_ == other.escape_ && null_string_ == other.null_string_ && AbstractScanPlanNode::operator==(rhs) &&
          AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::plan_node
