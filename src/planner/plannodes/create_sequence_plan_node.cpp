#include "planner/plannodes/create_sequence_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t CreateSequencePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash sequence name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_name_));

  // Hash sequence start
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_start_));

  // Hash sequence increment
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_increment_));

  // Hash sequence max
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_max_));

  // Hash sequence min
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_min_));

  // Hash sequence cycle
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_cycle_));

  return hash;
}

bool CreateSequencePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateSequencePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Sequence name
  if (GetSequenceName() != other.GetSequenceName()) return false;

  // Sequence start
  if (sequence_start_ != other.sequence_start_) return false;

  // Sequence increment
  if (sequence_increment_ != other.sequence_increment_) return false;

  // Sequence max
  if (sequence_max_ != other.sequence_max_) return false;

  // Sequence min
  if (sequence_min_ != other.sequence_min_) return false;

  // Sequence cycle
  if (sequence_cycle_ != other.sequence_cycle_) return false;

  return true;
}

nlohmann::json CreateSequencePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["sequence_name"] = sequence_name_;
  j["sequence_start"] = sequence_start_;
  j["sequence_increment"] = sequence_increment_;
  j["sequence_max"] = sequence_max_;
  j["sequence_min"] = sequence_min_;
  j["sequence_cycle"] = sequence_cycle_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateSequencePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  sequence_name_ = j.at("sequence_name").get<std::string>();
  sequence_start_ = j.at("sequence_start").get<int64_t>();
  sequence_increment_ = j.at("sequence_increment").get<int64_t>();
  sequence_max_ = j.at("sequence_max").get<int64_t>();
  sequence_min_ = j.at("sequence_min").get<int64_t>();
  sequence_cycle_ = j.at("sequence_cycle").get<bool>();
  return exprs;
}

}  // namespace terrier::planner
