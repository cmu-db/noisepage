#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/plan_node_defs.h"

namespace terrier::planner {

/**
 * An abstract plan node should be the base class for (almost) all plan nodes
 */
class AbstractPlanNode {
 protected:
  /**
   * Base builder class for plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder {
   public:
    Builder() = default;
    virtual ~Builder() = default;

    /**
     * @param child child to be added
     * @return builder object
     */
    ConcreteType &AddChild(std::shared_ptr<AbstractPlanNode> child) {
      children_.emplace_back(std::move(child));
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param output_schema output schema for plan node
     * @return builder object
     */
    ConcreteType &SetOutputSchema(const std::shared_ptr<OutputSchema> &output_schema) {
      output_schema_ = output_schema;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * child plans
     */
    std::vector<std::shared_ptr<AbstractPlanNode>> children_;
    /**
     * schema describing output of the node
     */
    std::shared_ptr<OutputSchema> output_schema_{nullptr};
  };

  /**
   * Constructor for the base AbstractPlanNode. Derived plan nodes should call this constructor to set output_schema
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  AbstractPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                   std::shared_ptr<OutputSchema> output_schema)
      : children_(std::move(children)), output_schema_(std::move(output_schema)) {}

 public:
  /**
   * Constructor for Deserialization and DDL statements
   */
  AbstractPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AbstractPlanNode)

  virtual ~AbstractPlanNode() = default;

  //===--------------------------------------------------------------------===//
  // Children Helpers
  //===--------------------------------------------------------------------===//

  /**
   * @return child plan nodes
   */
  const std::vector<std::shared_ptr<AbstractPlanNode>> &GetChildren() const { return children_; }

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
  virtual PlanNodeType GetPlanNodeType() const = 0;

  /**
   * @return output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   */
  std::shared_ptr<OutputSchema> GetOutputSchema() const { return output_schema_; }

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
   * @param j json to deserialize
   */
  virtual void FromJson(const nlohmann::json &j);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  /**
   * Derived plan nodes should call this method from their override of Hash() to hash data belonging to the base class
   * @return hash of the plan node
   */
  virtual common::hash_t Hash() const {
    // PlanNodeType
    common::hash_t hash = common::HashUtil::Hash(GetPlanNodeType());

    // OutputSchema
    if (output_schema_ != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, output_schema_->Hash());
    }

    // Children
    for (const auto &child : GetChildren()) {
      hash = common::HashUtil::CombineHashes(hash, child->Hash());
    }
    return hash;
  }

  /**
   * Perform a deep comparison of a plan node
   * @param rhs other node to compare against
   * @return true if plan node and its children are equal
   */
  virtual bool operator==(const AbstractPlanNode &rhs) const {
    if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

    // OutputSchema
    auto other_output_schema = rhs.GetOutputSchema();
    if ((output_schema_ == nullptr && other_output_schema != nullptr) ||
        (output_schema_ != nullptr && other_output_schema == nullptr)) {
      return false;
    }
    if (output_schema_ != nullptr && *output_schema_ != *other_output_schema) return false;

    // Children
    auto num = GetChildren().size();
    if (num != rhs.GetChildren().size()) return false;
    for (unsigned int i = 0; i < num; i++) {
      if (*GetChild(i) != *const_cast<AbstractPlanNode *>(rhs.GetChild(i))) return false;
    }
    return true;
  }

  /**
   * @param rhs other node to compare against
   * @return true if two plan nodes are not equivalent
   */
  bool operator!=(const AbstractPlanNode &rhs) const { return !(*this == rhs); }

 private:
  std::vector<std::shared_ptr<AbstractPlanNode>> children_;
  std::shared_ptr<OutputSchema> output_schema_;
};

DEFINE_JSON_DECLARATIONS(AbstractPlanNode);

/**
 * Main deserialization method. This is the only method that should be used to deserialize. You should never be calling
 * FromJson to deserialize a plan node
 * @param json json to deserialize
 * @return deserialized plan node
 */
std::shared_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json);

}  // namespace terrier::planner

namespace std {

/**
 * template for std::hash of plan nodes
 */
template <>
struct hash<std::shared_ptr<terrier::planner::AbstractPlanNode>> {
  /**
   * Hashes the given plan node
   * @param plan the plan to hash
   * @return hash code of the given plan node
   */
  size_t operator()(const std::shared_ptr<terrier::planner::AbstractPlanNode> &plan) const { return plan->Hash(); }
};

/**
 * std template for equality predicate
 */
template <>
struct equal_to<std::shared_ptr<terrier::planner::AbstractPlanNode>> {
  /**
   * @param lhs left hand side plan node
   * @param rhs right hand side plan node
   * @return true if plan nodes are equivalent
   */
  bool operator()(const std::shared_ptr<terrier::planner::AbstractPlanNode> &lhs,
                  const std::shared_ptr<terrier::planner::AbstractPlanNode> &rhs) const {
    return *lhs == *rhs;
  }
};

}  // namespace std
