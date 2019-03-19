#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_join_plan_node.h"

// TODO(Gus,Wen): Do we need left and right keys, or is this something you can figure out from the predicate?

namespace terrier::plan_node {

/**
 * Plan node for hash join. Hash joins are constructed so that the left is the probe table, and the right is the hashed
 * table
 */
class HashJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   * @param left_hash_keys left side keys to be hashed on
   * @param right_hash_keys right side keys to be hashed on
   * @param build_bloomfilter flag whether to build a bloom filter
   */
  HashJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                   parser::AbstractExpression *predicate, std::vector<parser::AbstractExpression *> left_hash_keys,
                   std::vector<parser::AbstractExpression *> right_hash_keys, bool build_bloomfilter = false)
      : AbstractJoinPlanNode(std::move(output_schema), join_type, predicate),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)) {}

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASHJOIN; }

  /**
   * @return true if bloom filtered flag is enabled
   */
  bool IsBloomFilterEnabled() const { return build_bloomfilter_; }

  /**
   * @return left side hash keys
   */
  const std::vector<parser::AbstractExpression *> &GetLeftHashKeys() const { return left_hash_keys_; }

  /**
   * @return right side hash keys
   */
  const std::vector<parser::AbstractExpression *> &GetRightHashKeys() const { return right_hash_keys_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // The left and right expressions that constitute the join keys
  const std::vector<parser::AbstractExpression *> left_hash_keys_;
  const std::vector<parser::AbstractExpression *> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;

 public:
  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode);
};

}  // namespace terrier::plan_node
