#include "planner/plannodes/create_index_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t CreateIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // TODO(Gus,Wen) Hash catalog::IndexSchema

  // Hash index_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_name_));

  return hash;
}

bool CreateIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateIndexPlanNode &>(rhs);

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Table OID
  if (table_oid_ != other.table_oid_) return false;

  // TODO(Gus,Wen) Compare catalog::IndexSchema

  // Index name
  if (index_name_ != other.index_name_) return false;

  return true;
}

nlohmann::json CreateIndexPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["index_name"] = index_name_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateIndexPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  index_name_ = j.at("index_name").get<std::string>();
  return exprs;
}

}  // namespace terrier::planner
