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

class HashPlanNode : public AbstractPlanNode {
 public:
  using HashKeyType = const parser::AbstractExpression;
  using HashKeyPtrType = std::shared_ptr<HashKeyType>;

  HashPlanNode(std::shared_ptr<OutputSchema> output_schema, std::vector<HashKeyPtrType> hash_keys)
      : AbstractPlanNode(std::move(output_schema)), hash_keys_(std::move(hash_keys)) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASH; }

  const std::string GetInfo() const override { return "HashPlanNode"; }

  const std::vector<HashKeyPtrType> &GetHashKeys() const { return hash_keys_; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    // Copy Hash keys
    std::vector<HashKeyPtrType> hash_keys;

    for (auto &key : GetHashKeys()) {
      hash_keys.push_back(key->Copy());
    }

    HashPlanNode *new_plan = new HashPlanNode(GetOutputSchema()->Copy(), hash_keys);
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  std::vector<HashKeyPtrType> hash_keys_;

 private:
  DISALLOW_COPY_AND_MOVE(HashPlanNode);
};

}  // namespace terrier::plan_node
