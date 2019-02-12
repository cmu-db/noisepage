#pragma once
#include <memory>
#include <vector>
#include "common/hash_util.h"
#include "common/macros.h"
#include "planner/planner_defs.h"

namespace terrier::planner {
/**
 * Base class for all PlanNodes
 */
class AbstractPlanNode {
 public:
  virtual ~AbstractPlanNode() = default;

  const std::vector<std::shared_ptr<AbstractPlanNode>> &GetChildren() const { return children_; }

  size_t GetChildrenSize() const { return children_.size(); }

  const std::shared_ptr<AbstractPlanNode> GetChild(uint32_t child_index) const {
    TERRIER_ASSERT(child_index < children_.size(), "child index access our of bounds");
    return children_[child_index];
  }

  const std::shared_ptr<AbstractPlanNode> GetParent() const { return parent_; }

  virtual PlanNodeType GetPlanNodeType() const = 0;

  virtual uint64_t Hash() const {
    uint64_t result = 0;
    for (auto &child : children_) result = common::HashUtil::CombineHashes(result, child->Hash());
    return result;
  }

  virtual bool operator==(const AbstractPlanNode &rhs) const {
    return GetPlanNodeType() == rhs.GetPlanNodeType() && parent_ == rhs.parent_ && children_ == rhs.children_;
  }

  bool operator!=(const AbstractPlanNode &rhs) const { return !(*this == rhs); }

 protected:
  AbstractPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                   std::shared_ptr<AbstractPlanNode> &&parent)
      : children_(std::move(children)), parent_(parent) {}

  template <class ConcreteType>
  class Builder {
   public:
    virtual ~Builder() = default;

    ConcreteType &SetParent(std::shared_ptr<AbstractPlanNode> parent) {
      parent_ = parent;
      return *dynamic_cast<ConcreteType *>(this);
    }

    ConcreteType &AddChild(std::shared_ptr<AbstractPlanNode> child) {
      children_.emplace_back(std::move(child));
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    std::vector<std::shared_ptr<AbstractPlanNode>> children_;
    std::shared_ptr<AbstractPlanNode> parent_;
  };

 private:
  // A plan node can have multiple children
  const std::vector<std::shared_ptr<AbstractPlanNode>> children_;
  const std::shared_ptr<AbstractPlanNode> parent_;
};
}  // namespace terrier::planner

namespace std {
template <>
struct hash<terrier::planner::AbstractPlanNode> {
 public:
  size_t operator()(const terrier::planner::AbstractPlanNode &node) const { return node.Hash(); }
};
}  // namespace std