#include "plan_node/export_external_file_plan_node.h"
#include <memory>
#include <string>

namespace terrier::plan_node {

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

}  // namespace terrier::plan_node
