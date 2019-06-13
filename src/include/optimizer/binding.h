#pragma once

#include <map>
#include <tuple>
#include <memory>

#include "loggers/optimizer_logger.h"

#include "optimizer/operator_node.h"
#include "optimizer/group.h"
#include "optimizer/pattern.h"
#include "optimizer/operator_expression.h"
#include "optimizer/memo.h"

namespace terrier {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Binding Iterator
//===--------------------------------------------------------------------===//
class BindingIterator {
 public:
  /**
   * Constructor for a binding iterator
   * @param memo Memo to be used
   */
  explicit BindingIterator(const Memo& memo) : memo_(memo) {}

  /**
   * Default destructor
   */
  virtual ~BindingIterator() = default;

  /**
   * Virtual function for whether a binding exists
   * @param Whether or not a binding still exists
   */
  virtual bool HasNext() = 0;

  /**
   * Virtual function for getting the next binding
   * @returns next OperatorExpression that matches
   */
  virtual OperatorExpression* Next() = 0;

 protected:
  const Memo &memo_;
};

class GroupBindingIterator : public BindingIterator {
 public:
  /**
   * Constructor for a group binding iterator
   * @param memo Memo to be used
   * @param id ID of the Group for binding
   * @param pattern Pattern to bind
   */
  GroupBindingIterator(const Memo& memo, GroupID id, Pattern* pattern)
    : BindingIterator(memo),
      group_id_(id),
      pattern_(pattern),
      target_group_(memo_.GetGroupByID(id)),
      num_group_items_(target_group_->GetLogicalExpressions().size()),
      current_item_index_(0) {
    OPTIMIZER_LOG_TRACE("Attempting to bind on group %d", id);
  }

  /**
   * Virtual function for whether a binding exists
   * @param Whether or not a binding still exists
   */
  bool HasNext() override;

  /**
   * Virtual function for getting the next binding
   * @returns next OperatorExpression that matches
   */
  OperatorExpression* Next() override;

 private:
  GroupID group_id_;
  Pattern* pattern_;
  Group *target_group_;
  size_t num_group_items_;

  size_t current_item_index_;
  std::unique_ptr<BindingIterator> current_iterator_;
};

class GroupExprBindingIterator : public BindingIterator {
 public:
  /**
   * Constructor for a GroupExpression binding iterator
   * @param memo Memo to be used
   * @param gexpr GroupExpression to bind to
   * @param pattern Pattern to bind
   */
  GroupExprBindingIterator(const Memo& memo, GroupExpression *gexpr, Pattern* pattern);

  /**
   * Destructor
   */
  ~GroupExprBindingIterator() override {
    for (auto &vec : children_bindings_) {
      for (auto expr : vec) {
        delete expr;
      }
    }

    if (current_binding_) {
      delete current_binding_;
    }
  }

  /**
   * Virtual function for whether a binding exists
   * @param Whether or not a binding still exists
   */
  bool HasNext() override;

  /**
   * Virtual function for getting the next binding
   * Pointer returned must be deleted by caller when done.
   * @returns next OperatorExpression that matches
   */
  OperatorExpression* Next() override {
    TERRIER_ASSERT(current_binding_, "binding must exist");
    auto binding = current_binding_;
    current_binding_ = nullptr;
    return binding;
  }

 private:
  GroupExpression* gexpr_;
  Pattern* pattern_;

  bool first_;
  bool has_next_;
  OperatorExpression* current_binding_;
  std::vector<std::vector<OperatorExpression*>> children_bindings_;
  std::vector<size_t> children_bindings_pos_;
};

} // namespace optimizer
} // namespace terrier
