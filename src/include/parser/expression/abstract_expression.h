#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "common/json.h"
#include "parser/expression/expression_defs.h"
#include "type/type_id.h"
#include "type/value.h"

namespace terrier {
namespace parser {
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
  AbstractExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : expression_type_(expression_type), return_value_type_(return_value_type), children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other)
      : expression_type_(other.expression_type_), return_value_type_(other.return_value_type_) {
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
      if (*children_[i] != *rhs.children_[i]) {
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
  type::TypeId GetReturnValueType() const { return return_value_type_; }

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
  const type::TypeId return_value_type_;                       // type of return value
  std::vector<std::unique_ptr<AbstractExpression>> children_;  // list of children
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
