#include "planner/plannodes/create_view_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {
common::hash_t CreateViewPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash namespace_oid
  auto namespace_oid = GetNamespaceOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid));

  // Hash view_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));

  // TODO(Gus,Wen) missing Hash for select statement

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateViewPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateViewPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Hash view_name
  if (GetViewName() != other.GetViewName()) return false;

  // TODO(Gus,Wen) missing == operator for select statement

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json CreateViewPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["view_name"] = view_name_;
  j["view_query"] = view_query_;
  return j;
}

void CreateViewPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  view_name_ = j.at("view_name").get<std::string>();
  if (!j.at("view_query").is_null()) {
    view_query_ = std::make_shared<parser::SelectStatement>();
    view_query_->FromJson(j.at("view_query"));
  }
}

}  // namespace terrier::planner
