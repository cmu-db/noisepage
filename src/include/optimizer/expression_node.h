#pragma once

#include <string>
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
  explicit ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents) {
    // TODO(esargent): bring over assert for contents being expression-based
    contents_ = contents;
  }

  /**
   * @return This ExpressionNode's child nodes.
   */
  const std::vector<common::ManagedPointer<AbstractOptimizerNode>> &GetChildren() const { return children_; }

  /**
   * @return The ExpressionNodeContents contained in this node.
   */
  const common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const {
    // TODO(esargent): bring over assert for contents being expression-based
    return contents_;
  }

  /**
   * @return A string representation of this node's information (currently, all nodes
   * have the empty string as their info)
   */
  const std::string GetInfo() const {
    // TODO(esargent): create proper info statement
    return "";
  }

 private:
  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;
  std::vector <common::ManagedPointer<AbstractOptimizerNode>> children_;
};

}  // namespace terrier::optimizer
