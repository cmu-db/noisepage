#include "planner/plannodes/alter_plan_node.h"
#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"

namespace terrier::planner {

common::hash_t AlterPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash column_oids
  hash = common::HashUtil::CombineHashInRange(hash, column_oids_.begin(), column_oids_.end());

  // Hash command pointers
  hash = common::HashUtil::CombineHashInRange(hash, cmds_.begin(), cmds_.end());

  return hash;
}

bool AlterPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AlterPlanNode &>(rhs);

  // Target table OID
  if (table_oid_ != other.table_oid_) return false;

  // Column Oids
  if (column_oids_ != other.column_oids_) return false;

  // Commands
  if (cmds_ != other.cmds_) return false;

  return true;
}

nlohmann::json AlterPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["table_oid"] = table_oid_;
  j["column_oids"] = column_oids_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AlterPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
  return exprs;
}
}  // namespace terrier::planner
