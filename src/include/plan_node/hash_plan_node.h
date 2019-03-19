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
  using HashKeyType = const parser::AbstractExpression;
  using HashKeyPtrType = std::shared_ptr<HashKeyType>;

  /**
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param hash_keys keys to be hashed on
   */
  HashPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<HashKeyPtrType> hash_keys)
      : AbstractPlanNode(std::move(output_schema)), hash_keys_(std::move(hash_keys)) {}

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASH; }

  /**
   * @return hash keys to be hashed on
   */
  const std::vector<HashKeyPtrType> &GetHashKeys() const { return hash_keys_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  std::vector<HashKeyPtrType> hash_keys_;

 public:
  DISALLOW_COPY_AND_MOVE(HashPlanNode);
};

}  // namespace terrier::plan_node
