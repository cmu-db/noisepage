#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_join_plan_node.h"

// TODO(Gus,Wen): Do we need left and right keys, or is this something you can figure out from the predicate?

namespace terrier::plan_node {

/**
 * Plan node for hash join. Hash joins are constructed so that the left is the probe table, and the right is the
 hashed
 * table
 */
class HashJoinPlanNode : public AbstractJoinPlanNode {
 protected:
  /**
   * Builder for hash join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    /**
     * Dont allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key key to add to left hash keys
     * @return builder object
     */
    Builder &AddLeftHashKey(parser::AbstractExpression *key) {
      left_hash_keys_.push_back(key);
      return *this;
    }

    /**
     * @param key key to add to right hash keys
     * @return builder object
     */
    Builder &AddRightHashKey(parser::AbstractExpression *key) {
      right_hash_keys_.push_back(key);
      return *this;
    }

    /**
     * @param flag build bloom filter flag
     * @return builder object
     */
    Builder &SetBuildBloomFilterFlag(bool flag) {
      build_bloomfilter_ = flag;
      return *this;
    }

    /**
     * Build the hash join plan node
     * @return plan node
     */
    std::shared_ptr<HashJoinPlanNode> Build() {
      return std::shared_ptr<HashJoinPlanNode>(
          new HashJoinPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_, join_type_,
                               std::move(predicate_), left_hash_keys_, right_hash_keys_, build_bloomfilter_));
    }

   protected:
    /**
     * left side hash keys
     */
    std::vector<parser::AbstractExpression *> left_hash_keys_;
    /**
     * right side hash keys
     */
    std::vector<parser::AbstractExpression *> right_hash_keys_;
    /**
     * if bloom filter should be built
     */
    bool build_bloomfilter_ = false;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param join_type logical join type
   * @param predicate join predicate
   * @param left_hash_keys left side keys to be hashed on
   * @param right_hash_keys right side keys to be hashed on
   * @param build_bloomfilter flag whether to build a bloom filter
   */
  HashJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                   std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                   LogicalJoinType join_type, std::unique_ptr<const parser::AbstractExpression> &&predicate,
                   std::vector<parser::AbstractExpression *> left_hash_keys,
                   std::vector<parser::AbstractExpression *> right_hash_keys, bool build_bloomfilter)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), estimated_cardinality, join_type,
                             std::move(predicate)),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)) {}

 public:
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

 private:
  // The left and right expressions that constitute the join keys
  const std::vector<parser::AbstractExpression *> left_hash_keys_;
  const std::vector<parser::AbstractExpression *> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;

 public:
  /**
   * Dont allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode);
};

}  // namespace terrier::plan_node
