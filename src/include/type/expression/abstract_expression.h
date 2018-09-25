#pragma once

#include <memory>
#include <vector>
#include "common/hash_util.h"
#include "common/json.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * An abstract parser expression. Dumb and immutable.
 */
class AbstractExpression {
 protected:
  /**
   * Instantiates a new abstract expression. Because these are logical expressions, everything should be known
   * at the time of instantiation, i.e. the resulting object is immutable.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param children the list of children for this node
   */
  AbstractExpression(const ExpressionType expression_type, const TypeId return_value_type,
                     const std::vector<AbstractExpression *> children)
      : expression_type_(expression_type), return_value_type_(return_value_type) {
    for (auto &child : children) {
      children_.emplace_back(std::unique_ptr<AbstractExpression>(child));
      if (child->is_nullable_) {
        is_nullable_ = true;
      }
      if (child->has_parameter_) {
        has_parameter_ = true;
      }
      if (child->has_subquery_) {
        has_subquery_ = true;
      }
    }
  }

  /**
   * (Convenience) Instantiates a new abstract expression for the 0-child case.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   */
  AbstractExpression(const ExpressionType expression_type, const TypeId return_value_type)
      : expression_type_(expression_type), return_value_type_(return_value_type) {}

  /**
   * (Convenience) Instantiates a new abstract expression for the 1-child case.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param child child
   */
  AbstractExpression(const ExpressionType expression_type, const TypeId return_value_type, AbstractExpression *child)
      : expression_type_(expression_type), return_value_type_(return_value_type) {
    if (child->is_nullable_) {
      is_nullable_ = true;
    }
    if (child->has_parameter_) {
      has_parameter_ = true;
    }
    if (child->has_subquery_) {
      has_subquery_ = true;
    }
    children_.emplace_back(std::unique_ptr<AbstractExpression>(child));
  }

  /**
   * (Convenience) Instantiates a new abstract expression for the 2-child case.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param left left child
   * @param right right child
   */
  AbstractExpression(const ExpressionType expression_type, const TypeId return_value_type, AbstractExpression *left,
                     AbstractExpression *right)
      : expression_type_(expression_type), return_value_type_(return_value_type) {
    if (left->is_nullable_ || right->is_nullable_) {
      is_nullable_ = true;
    }
    if (left->has_parameter_ || right->has_parameter_) {
      has_parameter_ = true;
    }
    if (left->has_subquery_ || right->has_subquery_) {
      has_subquery_ = true;
    }
    children_.emplace_back(std::unique_ptr<AbstractExpression>(left));
    children_.emplace_back(std::unique_ptr<AbstractExpression>(right));
  }

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other)
      : expression_type_(other.expression_type_),
        return_value_type_(other.return_value_type_),
        is_nullable_(other.is_nullable_),
        has_parameter_(other.has_parameter_),
        has_subquery_(other.has_subquery_) {
    for (auto const &child : other.children_) {
      children_.emplace_back(std::unique_ptr<AbstractExpression>(child->Copy()));
    }
  }

 public:
  virtual ~AbstractExpression() = default;

  /**
   * Hashes the current abstract expression.
   */
  virtual hash_t Hash() const {
    hash_t hash = HashUtil::Hash(&expression_type_);
    for (auto const &child : children_) {
      hash = HashUtil::CombineHashes(hash, child->Hash());
    }
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  virtual bool operator==(const AbstractExpression &rhs) const {
    if (expression_type_ != rhs.expression_type_ || children_.size() != rhs.children_.size()) {
      return false;
    }

    for (size_t i = 0; i < children_.size(); i++) {
      if (*children_[i].get() != *rhs.children_[i].get()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two expressions are not logically equal
   */
  virtual bool operator!=(const AbstractExpression &rhs) const { return !(*this == rhs); }

  /**
   * Creates a copy of the current AbstractExpression.
   */
  virtual AbstractExpression *Copy() const { return new AbstractExpression(*this); }

  /**
   * Returns the expression type.
   * @return the type of this expression
   */
  ExpressionType GetExpressionType() const { return expression_type_; }

  /**
   * Returns the return value type.
   * @return the type of the return value
   */
  TypeId GetReturnValueType() const { return return_value_type_; }

  /**
   * Return the number of children in this abstract expression.
   * @return number of children
   */
  size_t GetChildrenSize() const { return children_.size(); }

  /**
   * Returns a child of the abstract expression.
   * @param index index of child, NOT BOUNDS CHECKED
   * @return child
   */
  AbstractExpression *GetChild(size_t index) const { return children_[index].get(); }

 private:
  const ExpressionType expression_type_;                       // type of current expression
  const TypeId return_value_type_;                             // type of return value
  std::vector<std::unique_ptr<AbstractExpression>> children_;  // list of children

  bool is_nullable_;
  bool has_parameter_;
  bool has_subquery_;
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
