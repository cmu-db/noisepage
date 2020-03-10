#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node.h"

namespace terrier::optimizer {

/**
 * Wrapper for ExpressionNodeContents for the query rewriter.
 */
class ExpressionNode : public AbstractOptimizerNode {
 public:
  /**
   * Constructor that wraps an ExpressionNode around a provided ExpressionNodeContents.
   * @param contents The contents to be wrapped
   */
  explicit ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents) { contents_ = contents; }

  /**
   * Create an ExpressionNode
   * @param op an operator to bind to this OperatorNode node
   * @param children children of this OperatorNode
   */
  explicit ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents,
                          std::vector<std::unique_ptr<AbstractOptimizerNode>> &&children)
      : contents_(contents), children_(std::move(children)) {}

  /**
   * Pushes a child node onto this node's children
   * @param child a child node
   */
  void PushChild(std::unique_ptr<AbstractOptimizerNode> child) override { children_.emplace_back(std::move(child)); }

  /**
   * @return This ExpressionNode's child nodes.
   */
  const std::vector<common::ManagedPointer<AbstractOptimizerNode>> GetChildren() const override {
    std::vector<common::ManagedPointer<AbstractOptimizerNode>> result;
    result.reserve(children_.size());
    for (auto &i : children_) result.emplace_back(common::ManagedPointer(i->Copy().release()));
    return result;
  }

  /**
   * @return The ExpressionNodeContents contained in this node.
   */
  common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const override {
    // TODO(esargent): bring over assert for contents being expression-based
    return contents_;
  }

  /**
   * @return A string representation of this node's information (currently, all nodes
   * have the empty string as their info)
   */
  std::string GetInfo() const override {
    // TODO(esargent): create proper info statement
    return "";
  }

  std::unique_ptr<AbstractOptimizerNode> Copy() override {
    std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
    for (const auto &op : children_) {
      auto copy_node = dynamic_cast<ExpressionNode *>(op.get())->Copy();
      const auto abstract_child = dynamic_cast<AbstractOptimizerNode *>(copy_node.release());
      child.emplace_back(abstract_child);
    }
    return std::make_unique<ExpressionNode>(contents_, std::move(child));
  }

 private:
  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;
  std::vector<std::unique_ptr<AbstractOptimizerNode>> children_;
};

}  // namespace terrier::optimizer
