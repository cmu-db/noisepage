#include "optimizer/binding.h"

#include <memory>
#include <utility>
#include <vector>

#include "loggers/optimizer_logger.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer.h"

namespace terrier::optimizer {

bool GroupBindingIterator::HasNext() {
  if (pattern_->Type() == OpType::LEAF) {
    return current_item_index_ == 0;
  }

  if (current_iterator_) {
    // Check if still have bindings in current item
    if (!current_iterator_->HasNext()) {
      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }

  if (current_iterator_ == nullptr) {
    // Keep checking item iterators until we find a match
    while (current_item_index_ < num_group_items_) {
      auto gexpr = target_group_->GetLogicalExpressions()[current_item_index_];
      auto gexpr_it = new GroupExprBindingIterator(memo_, gexpr, pattern_, txn_);
      current_iterator_.reset(gexpr_it);

      if (current_iterator_->HasNext()) {
        break;
      }

      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }
  return current_iterator_ != nullptr;
}

std::unique_ptr<AbstractOptimizerNode> GroupBindingIterator::Next() {
  if (pattern_->Type() == OpType::LEAF) {
    current_item_index_ = num_group_items_;
    std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
    return std::make_unique<OperatorNode>(LeafOperator::Make(group_id_).RegisterWithTxnContext(txn_), std::move(c),
                                          txn_);
  }

  return current_iterator_->Next();
}

GroupExprBindingIterator::GroupExprBindingIterator(const Memo &memo, GroupExpression *gexpr, Pattern *pattern,
                                                   transaction::TransactionContext *txn)
    : BindingIterator(memo), gexpr_(gexpr), first_(true), has_next_(false), current_binding_(nullptr), txn_(txn) {
  if (gexpr->Contents()->GetOpType() != pattern->Type()) {
    // Check root node type
    return;
  }

  const std::vector<group_id_t> &child_groups = gexpr->GetChildGroupIDs();
  const std::vector<Pattern *> &child_patterns = pattern->Children();

  if (child_groups.size() != child_patterns.size()) {
    // Check make sure sizes are equal
    return;
  }

  OPTIMIZER_LOG_TRACE("Attempting to bind on group " + std::to_string(gexpr->GetGroupID().UnderlyingValue()) +
                      " with expression of type " + gexpr->Contents()->GetName() + ", children size " +
                      std::to_string(child_groups.size()));

  // Find all bindings for children
  children_bindings_.resize(child_groups.size());
  children_bindings_pos_.resize(child_groups.size(), 0);

  // Get first level children
  std::vector<std::unique_ptr<AbstractOptimizerNode>> children;

  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    std::vector<std::unique_ptr<AbstractOptimizerNode>> &child_bindings = children_bindings_[i];
    GroupBindingIterator iterator(memo_, child_groups[i], child_patterns[i], txn_);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.emplace_back(iterator.Next());
    }

    if (child_bindings.empty()) {
      // Child binding failed
      return;
    }

    // Push a copy
    children.emplace_back(child_bindings[0]->Copy());
  }

  has_next_ = true;
  current_binding_ = std::make_unique<OperatorNode>(gexpr->Contents(), std::move(children), txn_);
}

bool GroupExprBindingIterator::HasNext() {
  if (has_next_ && first_) {
    first_ = false;
    return true;
  }

  if (has_next_) {
    // The first child to be modified
    int first_modified_idx = static_cast<int>(children_bindings_pos_.size()) - 1;
    for (; first_modified_idx >= 0; --first_modified_idx) {
      const std::vector<std::unique_ptr<AbstractOptimizerNode>> &child_binding = children_bindings_[first_modified_idx];

      // Try to increment idx from the back
      size_t new_pos = ++children_bindings_pos_[first_modified_idx];
      if (new_pos >= child_binding.size()) {
        children_bindings_pos_[first_modified_idx] = 0;
      } else {
        break;
      }
    }

    if (first_modified_idx < 0) {
      // We have explored all combinations of the child bindings
      has_next_ = false;
    } else {
      std::vector<std::unique_ptr<AbstractOptimizerNode>> children;
      for (size_t idx = 0; idx < children_bindings_pos_.size(); ++idx) {
        const std::vector<std::unique_ptr<AbstractOptimizerNode>> &child_binding = children_bindings_[idx];
        children.emplace_back(child_binding[children_bindings_pos_[idx]]->Copy());
      }

      TERRIER_ASSERT(!current_binding_, "Next() should have been called");
      current_binding_ = std::make_unique<OperatorNode>(gexpr_->Contents(), std::move(children), txn_);
    }
  }

  return has_next_;
}

}  // namespace terrier::optimizer
