#include "plan_node/update_plan_node.h"
#include <memory>
#include <utility>
#include "plan_node/abstract_scan_plan_node.h"
#include "plan_node/output_schema.h"
#include "storage/sql_table.h"

namespace terrier::plan_node {

// TODO(Gus,Wen) initialize update_primary_key by checking SQL table schema
UpdatePlanNode::UpdatePlanNode(catalog::table_oid_t target_table_oid, std::shared_ptr<OutputSchema> output_schema)
    : AbstractPlanNode(std::move(output_schema)), target_table_oid_(target_table_oid), update_primary_key_(false) {}

common::hash_t UpdatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  auto target_table_oid = GetTargetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&target_table_oid));

  auto is_update_primary_key = GetUpdatePrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_update_primary_key));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool UpdatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const plan_node::UpdatePlanNode &>(rhs);
  if (GetTargetTableOid() != other.GetTargetTableOid()) return false;

  // Update primary key
  if (GetUpdatePrimaryKey() != other.GetUpdatePrimaryKey()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
