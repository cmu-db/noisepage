#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

/**
 * Wrapper for ExpressionNodeContents for the query rewriter.
 */
class ExpressionNode : public AbstractOptimizerNode {
 public:
  /**
   * Constructor that wraps an ExpressionNode around a provided ExpressionNodeContents.
   * @param contents The contents to be wrapped
   * @param txn transaction context for managing memory
   */
  explicit ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents,
                          transaction::TransactionContext *txn)
      : contents_(contents), txn_(txn) {}

  /**
   * Create an ExpressionNode
   * @param contents the contents to bind to this expression node
   * @param children children of this ExpressionNode
   * @param txn transaction context for managing memory
   */
  explicit ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents,
                          std::vector<std::unique_ptr<AbstractOptimizerNode>> &&children,
                          transaction::TransactionContext *txn)
      : contents_(contents), children_(std::move(children)), txn_(txn) {}

  /**
   * Pushes a child node onto this node's children
   * @param child a child node
   */
  void PushChild(std::unique_ptr<AbstractOptimizerNode> child) override { children_.emplace_back(std::move(child)); }

  /**
   * @return This ExpressionNode's child nodes.
   */
  std::vector<common::ManagedPointer<AbstractOptimizerNode>> GetChildren() const override {
    std::vector<common::ManagedPointer<AbstractOptimizerNode>> result;
    result.reserve(children_.size());
    for (auto &child : children_) {
      result.emplace_back(common::ManagedPointer(child));
    }
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
   * @return a copy of this expression node (as an AbstractOptimizerNode ptr)
   */
  std::unique_ptr<AbstractOptimizerNode> Copy() override {
    std::vector<std::unique_ptr<AbstractOptimizerNode>> copy_children;
    for (const auto &child : children_) {
      auto *copy_node = dynamic_cast<ExpressionNode *>(child.get())->Copy().release();
      const auto abstract_child = dynamic_cast<AbstractOptimizerNode *>(copy_node);
      if (txn_ != nullptr) {
        txn_->RegisterCommitAction([=]() { delete copy_node; });
        txn_->RegisterAbortAction([=]() { delete copy_node; });
      }
      copy_children.emplace_back(abstract_child);
    }
    return std::make_unique<ExpressionNode>(contents_, std::move(copy_children), txn_.Get());
  }

 private:
  /**
   * contents to bind to this node
   */
  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;

  /**
   * vector of child nodes
   */
  std::vector<std::unique_ptr<AbstractOptimizerNode>> children_;

  /**
   * Transaction context for managing memory
   */
  common::ManagedPointer<transaction::TransactionContext> txn_;
};

}  // namespace noisepage::optimizer
