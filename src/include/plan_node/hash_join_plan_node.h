#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_join_plan_node.h"

// TODO(Gus,Wen): Do we need left and right keys, or is this something you can figure out from the predicate?

namespace terrier::plan_node {

class HashJoinPlanNode : public AbstractJoinPlanNode {
 public:
  HashJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                   parser::AbstractExpression *predicate, std::vector<parser::AbstractExpression *> left_hash_keys,
                   std::vector<parser::AbstractExpression *> right_hash_keys, bool build_bloomfilter = false)
      : AbstractJoinPlanNode(std::move(output_schema), join_type, predicate),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASHJOIN; }

  bool IsBloomFilterEnabled() const { return build_bloomfilter_; }

  const std::vector<parser::AbstractExpression *> &GetLeftHashKeys() const { return left_hash_keys_; }

  const std::vector<parser::AbstractExpression *> &GetRightHashKeys() const { return right_hash_keys_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utils
  ///
  //////////////////////////////////////////////////////////////////////////////

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // The left and right expressions that constitute the join keys
  const std::vector<parser::AbstractExpression *> left_hash_keys_;
  const std::vector<parser::AbstractExpression *> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;

 private:
  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode);
};

}  // namespace terrier::plan_node
