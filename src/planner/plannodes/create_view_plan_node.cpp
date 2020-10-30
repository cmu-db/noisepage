#include "planner/plannodes/create_view_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<CreateViewPlanNode> CreateViewPlanNode::Builder::Build() {
  return std::unique_ptr<CreateViewPlanNode>(new CreateViewPlanNode(std::move(children_), std::move(output_schema_),
                                                                    database_oid_, namespace_oid_,
                                                                    std::move(view_name_), std::move(view_query_)));
}

CreateViewPlanNode::CreateViewPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                       std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                                       catalog::namespace_oid_t namespace_oid, std::string view_name,
                                       std::unique_ptr<parser::SelectStatement> view_query)
    : AbstractPlanNode(std::move(children), std::move(output_schema)),
      database_oid_(database_oid),
      namespace_oid_(namespace_oid),
      view_name_(std::move(view_name)),
      view_query_(std::move(view_query)) {}

common::hash_t CreateViewPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash view_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));

  // Hash view query
  if (view_query_ != nullptr) hash = common::HashUtil::CombineHashes(hash, view_query_->Hash());
  return hash;
}

bool CreateViewPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateViewPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // View name
  if (GetViewName() != other.GetViewName()) return false;

  // View query
  if (view_query_ != nullptr) {
    if (other.view_query_ == nullptr) return false;
    if (*view_query_ != *other.view_query_) return false;
  }
  if (view_query_ == nullptr && other.view_query_ != nullptr) return false;

  return true;
}

nlohmann::json CreateViewPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["view_name"] = view_name_;
  j["view_query"] = view_query_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateViewPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  view_name_ = j.at("view_name").get<std::string>();
  if (!j.at("view_query").is_null()) {
    view_query_ = std::make_unique<parser::SelectStatement>();
    auto e2 = view_query_->FromJson(j.at("view_query"));
    exprs.insert(exprs.end(), std::make_move_iterator(e2.begin()), std::make_move_iterator(e2.end()));
  }
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(CreateViewPlanNode);

}  // namespace noisepage::planner
