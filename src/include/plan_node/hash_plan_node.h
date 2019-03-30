#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"

// TODO(Gus,Wen): Replace PerformBinding and VisitParameters
// TODO(Gus,Wen): Does this plan really output columns? This might be a special case

namespace terrier::plan_node {

/**
 * Plan node for a hash operator
 */
class HashPlanNode : public AbstractPlanNode {
 public:
  /**
   * Hash keys are AsbtractExpressions
   */
  using HashKeyType = const parser::AbstractExpression;

  /**
   * Builder for hash plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param key Hash key to be added
     * @return builder object
     */
    Builder &AddHashKey(std::shared_ptr<HashKeyType> key) {
      hash_keys_.emplace_back(key);
      return *this;
    }

    /**
     * Build the Hash plan node
     * @return plan node
     */
    std::shared_ptr<HashPlanNode> Build() {
      return std::shared_ptr<HashPlanNode>(
          new HashPlanNode(std::move(children_), std::move(output_schema_), std::move(hash_keys_)));
    }

   protected:
    /**
     * keys to be hashed on
     */
    std::vector<std::shared_ptr<HashKeyType>> hash_keys_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param hash_keys keys to be hashed on
   */
  HashPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
               std::vector<std::shared_ptr<HashKeyType>> hash_keys)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), hash_keys_(std::move(hash_keys)) {}

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASH; }

  /**
   * @return keys to be hashed on
   */
  const std::vector<std::shared_ptr<HashKeyType>> &GetHashKeys() const { return hash_keys_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  std::vector<std::shared_ptr<HashKeyType>> hash_keys_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(HashPlanNode);
};

}  // namespace terrier::plan_node
