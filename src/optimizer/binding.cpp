#include "optimizer/binding.h"

#include "loggers/optimizer_logger.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer.h"

namespace terrier {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group Binding Iterator
//===--------------------------------------------------------------------===//
bool GroupBindingIterator::HasNext() {
  OPTIMIZER_LOG_TRACE("HasNext");
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
      auto gexpr_it = new GroupExprBindingIterator(memo_, gexpr, pattern_);
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

OperatorExpression* GroupBindingIterator::Next() {
  if (pattern_->Type() == OpType::LEAF) {
    current_item_index_ = num_group_items_;
    return new OperatorExpression(LeafOperator::make(group_id_), {});
  }

  return current_iterator_->Next();
}

//===--------------------------------------------------------------------===//
// Item Binding Iterator
//===--------------------------------------------------------------------===//
GroupExprBindingIterator::GroupExprBindingIterator(
    const Memo &memo, GroupExpression *gexpr, Pattern* pattern)
    : BindingIterator(memo),
      gexpr_(gexpr),
      pattern_(pattern),
      first_(true),
      has_next_(false),
      current_binding_(nullptr) {

  if (gexpr->Op().GetType() != pattern->Type()) {
    // Check root node type
    return;
  }

  const std::vector<GroupID> &child_groups = gexpr->GetChildGroupIDs();
  const std::vector<Pattern*> &child_patterns = pattern->Children();

  if (child_groups.size() != child_patterns.size()) {
    // Check make sure sizes are equal
    return;
  }

  OPTIMIZER_LOG_TRACE(
      "Attempting to bind on group %d with expression of type %s, children size %lu",
      gexpr->GetGroupID(), gexpr->Op().GetName().c_str(), child_groups.size());

  // Find all bindings for children
  children_bindings_.resize(child_groups.size(), {});
  children_bindings_pos_.resize(child_groups.size(), 0);

  // Get first level children
  std::vector<OperatorExpression*> children;

  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    std::vector<OperatorExpression*> &child_bindings = children_bindings_[i];
    GroupBindingIterator iterator(memo_, child_groups[i], child_patterns[i]);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.push_back(iterator.Next());
    }

    if (child_bindings.size() == 0) {
      // Child binding failed
      return;
    }

    // PUsh a copy
    children.push_back(child_bindings[0]->Copy());
  }

  has_next_ = true;
  current_binding_ = new OperatorExpression(gexpr->Op(), std::move(children));
}

bool GroupExprBindingIterator::HasNext() {
  OPTIMIZER_LOG_TRACE("HasNext");
  if (has_next_ && first_) {
    first_ = false;
    return true;
  }

  if (has_next_) {
    // The first child to be modified
    int first_modified_idx = static_cast<int>(children_bindings_pos_.size()) - 1;
    for (; first_modified_idx >= 0; --first_modified_idx) {
      const std::vector<OperatorExpression*> &child_binding = children_bindings_[first_modified_idx];

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
      std::vector<OperatorExpression*> children;
      for (size_t idx = 0; idx < children_bindings_pos_.size(); ++idx) {
        const std::vector<OperatorExpression*> &child_binding = children_bindings_[idx];
        children.push_back(child_binding[children_bindings_pos_[idx]]->Copy());
      }

      TERRIER_ASSERT(!current_binding_, "Next() should have been called");
      current_binding_ = new OperatorExpression(gexpr_->Op(), std::move(children));
    }
  }

  return has_next_;
}

}  // namespace optimizer
}  // namespace terrier
