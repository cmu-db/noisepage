#include "planner/plannodes/order_by_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<OrderByPlanNode> OrderByPlanNode::Builder::Build() {
  return std::unique_ptr<OrderByPlanNode>(new OrderByPlanNode(std::move(children_), std::move(output_schema_),
                                                              std::move(sort_keys_), has_limit_, limit_, offset_,
                                                              plan_node_id_));
}

OrderByPlanNode::OrderByPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                 std::unique_ptr<OutputSchema> output_schema, std::vector<SortKey> sort_keys,
                                 bool has_limit, size_t limit, size_t offset, plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id),
      sort_keys_(std::move(sort_keys)),
      has_limit_(has_limit),
      limit_(limit),
      offset_(offset) {}

common::hash_t OrderByPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Sort Keys
  for (const auto &sort_key : sort_keys_) {
    if (sort_key.first != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, sort_key.first->Hash());
    }
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sort_key.second));
  }

  // Inlined Limit Stuff
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(has_limit_));
  if (has_limit_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
  }

  return hash;
}

bool OrderByPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const OrderByPlanNode &>(rhs);

  // Sort Keys
  if (sort_keys_.size() != other.sort_keys_.size()) return false;
  for (auto i = 0U; i < sort_keys_.size(); i++) {
    auto &sort_key = sort_keys_[i];
    auto &other_sort_key = other.sort_keys_[i];
    if (sort_key.second != other_sort_key.second) return false;
    if ((sort_key.first == nullptr && other_sort_key.first != nullptr) ||
        (sort_key.first != nullptr && other_sort_key.first == nullptr))
      return false;
    if (sort_key.first != nullptr && *sort_key.first != *other_sort_key.first) return false;
  }

  //  Inlined Limit Stuff
  if (has_limit_ != other.has_limit_) return false;
  if (has_limit_) {
    // Limit
    if (limit_ != other.limit_) return false;

    // Offset
    if (offset_ != other.offset_) return false;
  }

  return true;
}

nlohmann::json OrderByPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();

  std::vector<std::pair<nlohmann::json, optimizer::OrderByOrderingType>> sort_keys;
  sort_keys.reserve(sort_keys_.size());
  for (const auto &key : sort_keys_) {
    sort_keys.emplace_back(key.first->ToJson(), key.second);
  }
  j["sort_keys"] = sort_keys;
  j["has_limit"] = has_limit_;
  j["limit"] = limit_;
  j["offset"] = offset_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> OrderByPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));

  // Deserialize sort keys
  auto sort_keys = j.at("sort_keys").get<std::vector<std::pair<nlohmann::json, optimizer::OrderByOrderingType>>>();
  for (const auto &key_json : sort_keys) {
    auto deserialized = parser::DeserializeExpression(key_json.first);
    sort_keys_.emplace_back(common::ManagedPointer(deserialized.result_), key_json.second);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  has_limit_ = j.at("has_limit").get<bool>();
  limit_ = j.at("limit").get<size_t>();
  offset_ = j.at("offset").get<size_t>();
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(OrderByPlanNode);

}  // namespace noisepage::planner
