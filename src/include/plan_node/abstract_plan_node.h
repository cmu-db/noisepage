#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "plan_node/output_schema.h"
#include "plan_node/plan_node_defs.h"

namespace terrier::plan_node {

/**
 * An abstract plan node should be the base class for (almost) all plan nodes
 */
class AbstractPlanNode {
 public:
  /**
   * Constructor for the base AbstractPlanNode. Derived plan nodes should call this constructor to set output_schema
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  explicit AbstractPlanNode(std::shared_ptr<OutputSchema> output_schema) : output_schema_(std::move(output_schema)) {}

  /**
   * Constructor for Deserialization and DDL statements
   */
  AbstractPlanNode() = default;

  virtual ~AbstractPlanNode() = default;

  //===--------------------------------------------------------------------===//
  // Children Helpers
  //===--------------------------------------------------------------------===//

  /**
   * Add a child plan node. Plan nodes are immutable, so this method should only be called during initialization of the
   * plan node
   * @param child child to be added
   */
  void AddChild(std::unique_ptr<AbstractPlanNode> &&child) { children_.emplace_back(std::move(child)); }

  /**
   * @return child plan nodes
   */
  const std::vector<std::unique_ptr<AbstractPlanNode>> &GetChildren() const { return children_; }

  /**
   * @return number of children
   */
  size_t GetChildrenSize() const { return children_.size(); }

  /**
   * @param child_index index of child
   * @return child at provided index
   */
  const AbstractPlanNode *GetChild(uint32_t child_index) const {
    TERRIER_ASSERT(child_index < children_.size(),
                   "index into children of plan node should be less than number of children");
    return children_[child_index].get();
  }

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  /**
   * Returns plan type, each derived plan class should override this method to return their specific type
   * @return plan type
   */
  virtual PlanNodeType GetPlanNodeType() const { return PlanNodeType::ABSTRACTPLAN; }

  /**
   * @return output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   */
  std::shared_ptr<OutputSchema> GetOutputSchema() const { return output_schema_; }

  /**
   * @return estimated cardinality of the output tuple set from this plan node
   */
  int GetEstimatedCardinality() const { return estimated_cardinality_; }

  /**
   * @param cardinality estimated cardinality to be set
   */
  void SetEstimatedCardinality(int cardinality) { estimated_cardinality_ = cardinality; }

  //===--------------------------------------------------------------------===//
  // JSON Serialization/Deserialization
  //===--------------------------------------------------------------------===//

  /**
   * Return the current plan node in JSON format.
   * @return JSON representation of plan node
   */
  virtual nlohmann::json ToJson() const;

  /**
   * Populates the plan node with the information in the given JSON.
   * Undefined behavior occurs if the JSON has a different PlanNodeType.
   */
  virtual void FromJson(const nlohmann::json &json);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  /**
   * Derived plan nodes should call this method from their override of Hash() to hash data belonging to the base class
   * @return hash of the plan node
   */
  virtual common::hash_t Hash() const {
    common::hash_t hash = GetOutputSchema()->Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetPlanNodeType()));
    for (auto &child : GetChildren()) {
      hash = common::HashUtil::CombineHashes(hash, child->Hash());
    }
    return hash;
  }

  virtual bool operator==(const AbstractPlanNode &rhs) const {
    if (*GetOutputSchema() != *rhs.GetOutputSchema()) return false;
    auto num = GetChildren().size();
    if (num != rhs.GetChildren().size()) return false;
    for (unsigned int i = 0; i < num; i++) {
      if (*GetChild(i) != *const_cast<AbstractPlanNode *>(rhs.GetChild(i))) return false;
    }
    return true;
  }

  virtual bool operator!=(const AbstractPlanNode &rhs) const { return !(*this == rhs); }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlanNode>> children_;

  int estimated_cardinality_ = 500000;

  std::shared_ptr<OutputSchema> output_schema_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractPlanNode);
};

class Equal {
 public:
  bool operator()(const std::shared_ptr<plan_node::AbstractPlanNode> &a,
                  const std::shared_ptr<plan_node::AbstractPlanNode> &b) const {
    return *a == *b;
  }
};

class Hash {
 public:
  size_t operator()(const std::shared_ptr<plan_node::AbstractPlanNode> &plan) const {
    return static_cast<size_t>(plan->Hash());
  }
};

// JSON library interface. Do not modify.
DEFINE_JSON_DECLARATIONS(AbstractPlanNode);
std::unique_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json);

}  // namespace terrier::plan_node
