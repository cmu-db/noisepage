#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/operator_node_contents.h"
#include "transaction/transaction_context.h"
namespace noisepage::optimizer {

/**
 * This class is used to represent nodes in the operator tree. The operator tree is generated
 * by the binder by visiting the abstract syntax tree (AST) produced by the parser and servers
 * as the input to the query optimizer.
 */
class OperatorNode : public AbstractOptimizerNode {
 public:
  /**
   * Create an OperatorNode
   * @param contents an AbstractOperatorNodeContents to bind to this node
   * @param children children of this OperatorNode
   * @param txn transaction context for memory management
   */
  explicit OperatorNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents,
                        std::vector<std::unique_ptr<AbstractOptimizerNode>> &&children,
                        transaction::TransactionContext *txn)
      : contents_(contents), children_(std::move(children)), txn_(common::ManagedPointer(txn)) {}

  /**
   * Operator-based constructor for an OperatorNode
   * @param op an operator to bind to this OperatorNode
   * @param children Children of this OperatorNode
   * @param txn transaction context for memory management
   */
  explicit OperatorNode(Operator op, std::vector<std::unique_ptr<AbstractOptimizerNode>> &&children,
                        transaction::TransactionContext *txn)
      : contents_(common::ManagedPointer<AbstractOptimizerNodeContents>(new Operator(std::move(op)))),
        children_(std::move(children)),
        txn_(common::ManagedPointer(txn)) {
    auto *op_node = reinterpret_cast<Operator *>(contents_.Get());
    if (txn_ != nullptr) {
      txn_->RegisterCommitAction([=]() { delete op_node; });
      txn_->RegisterAbortAction([=]() { delete op_node; });
    }
  }

  /**
   * Default destructor
   */
  ~OperatorNode() override = default;

  /**
   * Copy
   */
  std::unique_ptr<AbstractOptimizerNode> Copy() override {
    std::vector<std::unique_ptr<AbstractOptimizerNode>> new_children;
    for (auto &op : children_) {
      NOISEPAGE_ASSERT(op != nullptr, "OperatorNode should not have null children");
      NOISEPAGE_ASSERT(op->Contents()->GetOpType() != OpType::UNDEFINED, "OperatorNode should have operator children");

      new_children.emplace_back(op->Copy());
    }
    auto result = std::make_unique<OperatorNode>(contents_, std::move(new_children), txn_.Get());
    return std::move(result);
  }

  /**
   * Equality comparison
   * @param other OperatorNode to compare against
   * @returns true if equal
   */
  bool operator==(const OperatorNode &other) const {
    if (contents_->GetOpType() != other.contents_->GetOpType()) return false;
    if (contents_->GetExpType() != other.contents_->GetExpType()) return false;
    if (children_.size() != other.children_.size()) return false;

    for (size_t idx = 0; idx < children_.size(); idx++) {
      auto &child = children_[idx];
      auto &other_child = other.children_[idx];

      NOISEPAGE_ASSERT(child != nullptr, "OperatorNode should not have null children");
      NOISEPAGE_ASSERT(child->Contents()->GetOpType() != OpType::UNDEFINED,
                       "OperatorNode should have operator children");
      NOISEPAGE_ASSERT(other_child != nullptr, "OperatorNode should not have null children");
      NOISEPAGE_ASSERT(other_child->Contents()->GetOpType() != OpType::UNDEFINED,
                       "OperatorNode should have operator children");

      auto *child_op = dynamic_cast<OperatorNode *>(child.get());
      auto *other_child_op = dynamic_cast<OperatorNode *>(other_child.get());

      if (*child_op != *other_child_op) return false;
    }
    return true;
  }

  /**
   * Not equal comparison
   * @param other OperatorNode to compare against
   * @returns true if not equal
   */
  bool operator!=(const OperatorNode &other) const { return !(*this == other); }

  /**
   * Move constructor
   * @param op other to construct from
   */
  OperatorNode(OperatorNode &&op) noexcept : contents_(op.contents_), children_(std::move(op.children_)) {}

  /**
   * @return vector of children
   */
  std::vector<common::ManagedPointer<AbstractOptimizerNode>> GetChildren() const override {
    std::vector<common::ManagedPointer<AbstractOptimizerNode>> result;
    for (const std::unique_ptr<AbstractOptimizerNode> &child : children_) {
      result.emplace_back(common::ManagedPointer(child));
    }
    return result;
  }

  /**
   * @return underlying operator
   */
  common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const override { return contents_; }

  /**
   * Add a operator expression as child
   * @param child The operator expression to be added as child
   */
  void PushChild(std::unique_ptr<AbstractOptimizerNode> child) override { children_.emplace_back(std::move(child)); }

 private:
  /**
   * Underlying operator
   */
  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;

  /**
   * Vector of children
   */
  std::vector<std::unique_ptr<AbstractOptimizerNode>> children_;

  /**
   * Transaction context for managing memory, both in terms of eventually freeing the on-the-fly operators created
   * and for eventually freeing copies made of this node.
   */
  common::ManagedPointer<transaction::TransactionContext> txn_;
};

}  // namespace noisepage::optimizer
