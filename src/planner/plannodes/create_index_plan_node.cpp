#include "planner/plannodes/create_index_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<CreateIndexPlanNode> CreateIndexPlanNode::Builder::Build() {
  return std::unique_ptr<CreateIndexPlanNode>(new CreateIndexPlanNode(std::move(children_), std::move(output_schema_),
                                                                      namespace_oid_, table_oid_,
                                                                      std::move(index_name_), std::move(schema_)));
}

CreateIndexPlanNode::CreateIndexPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                         std::unique_ptr<OutputSchema> output_schema,
                                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                                         std::string index_name, std::unique_ptr<catalog::IndexSchema> schema)
    : AbstractPlanNode(std::move(children), std::move(output_schema)),
      namespace_oid_(namespace_oid),
      table_oid_(table_oid),
      index_name_(std::move(index_name)),
      schema_(std::move(schema)) {}

common::hash_t CreateIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash index schema
  if (schema_ != nullptr) hash = common::HashUtil::CombineHashes(hash, schema_->Hash());

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

  // Index schema
  if (schema_ != nullptr) {
    if (other.schema_ == nullptr) return false;
    if (*schema_ != *other.schema_) return false;
  }
  if (schema_ == nullptr && other.schema_ != nullptr) return false;

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
DEFINE_JSON_BODY_DECLARATIONS(CreateIndexPlanNode);

}  // namespace noisepage::planner
