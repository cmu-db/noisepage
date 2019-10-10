#include "planner/plannodes/delete_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

namespace terrier::planner {

// TODO(Gus,Wen) Add SetParameters

common::hash_t DeletePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash delete_condition
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delete_condition_->Hash()));

  return hash;
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DeletePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Table OID
  if (table_oid_ != other.table_oid_) return false;

  // Delete condition
  if (*GetDeleteCondition() != *other.GetDeleteCondition()) return false;

  return true;
}

nlohmann::json DeletePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["delete_condition"] = delete_condition_ == nullptr ? nlohmann::json(nullptr) : delete_condition_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DeletePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  if (!j.at("delete_condition").is_null()) {
    auto deserialized = parser::DeserializeExpression(j.at("delete_condition"));
    delete_condition_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  return exprs;
}

}  // namespace terrier::planner
