#include "plan_node/csv_scan_plan_node.h"
#include <memory>
#include <string>

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> CSVScanPlanNode::Copy() const {
  return std::unique_ptr<AbstractPlanNode>(
      new CSVScanPlanNode(GetOutputSchema(), file_name_, delimiter_, quote_, escape_, null_string_));
}

common::hash_t CSVScanPlanNode::Hash() const {
  common::hash_t hash = std::hash<std::string>{}(file_name_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&delimiter_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&quote_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&escape_));
  hash = common::HashUtil::CombineHashes(hash, std::hash<std::string>{}(null_string_));
  hash = common::HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  return hash;
}

// TODO(Gus): Are file names case sensitive?
bool CSVScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::CSVSCAN) return false;
  const auto &other = static_cast<const CSVScanPlanNode &>(rhs);
  return (file_name_ == other.file_name_ && delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
          escape_ == other.escape_ && null_string_ == other.null_string_);
}

}  // namespace terrier::plan_node
