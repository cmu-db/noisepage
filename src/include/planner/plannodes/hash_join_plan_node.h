#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for hash join. Hash joins are constructed so that the left is the probe table, and the right is the
 hashed
 * table
 */
class HashJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for hash join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key key to add to left hash keys
     * @return builder object
     */
    Builder &AddLeftHashKey(std::shared_ptr<parser::AbstractExpression> key) {
      left_hash_keys_.emplace_back(std::move(key));
      return *this;
    }

    /**
     * @param key key to add to right hash keys
     * @return builder object
     */
    Builder &AddRightHashKey(std::shared_ptr<parser::AbstractExpression> key) {
      right_hash_keys_.emplace_back(std::move(key));
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
          new HashJoinPlanNode(std::move(children_), std::move(output_schema_), join_type_, std::move(join_predicate_),
                               left_hash_keys_, right_hash_keys_, build_bloomfilter_));
    }

   protected:
    /**
     * left side hash keys
     */
    std::vector<std::shared_ptr<parser::AbstractExpression>> left_hash_keys_;
    /**
     * right side hash keys
     */
    std::vector<std::shared_ptr<parser::AbstractExpression>> right_hash_keys_;
    /**
     * if bloom filter should be built
     */
    bool build_bloomfilter_ = false;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   * @param left_hash_keys left side keys to be hashed on
   * @param right_hash_keys right side keys to be hashed on
   * @param build_bloomfilter flag whether to build a bloom filter
   */
  HashJoinPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                   std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                   std::shared_ptr<parser::AbstractExpression> predicate,
                   std::vector<std::shared_ptr<parser::AbstractExpression>> left_hash_keys,
                   std::vector<std::shared_ptr<parser::AbstractExpression>> right_hash_keys, bool build_bloomfilter)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, std::move(predicate)),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)),
        build_bloomfilter_(build_bloomfilter) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  HashJoinPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode)

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
  const std::vector<std::shared_ptr<parser::AbstractExpression>> &GetLeftHashKeys() const { return left_hash_keys_; }

  /**
   * @return right side hash keys
   */
  const std::vector<std::shared_ptr<parser::AbstractExpression>> &GetRightHashKeys() const { return right_hash_keys_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  // The left and right expressions that constitute the join keys
  std::vector<std::shared_ptr<parser::AbstractExpression>> left_hash_keys_;
  std::vector<std::shared_ptr<parser::AbstractExpression>> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;
};

DEFINE_JSON_DECLARATIONS(HashJoinPlanNode);

}  // namespace terrier::planner
