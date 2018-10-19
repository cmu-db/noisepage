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
                     std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : expression_type_(expression_type), return_value_type_(return_value_type), children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other)
      : expression_type_(other.expression_type_), return_value_type_(other.return_value_type_) {
    for (auto const &child : other.children_) {
      children_.emplace_back(child->Copy());
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
  virtual std::unique_ptr<AbstractExpression> Copy() const {
    std::vector<std::shared_ptr<AbstractExpression>> children;
    for (auto const &child : children_) {
      children.emplace_back(child->Copy());
    }
    return std::unique_ptr<AbstractExpression>(
        new AbstractExpression(expression_type_, return_value_type_, std::move(children)));
  }

  /**
   * @return type of this expression
   */
  ExpressionType GetExpressionType() const { return expression_type_; }

  /**
   * @return type of the return value
   */
  type::TypeId GetReturnValueType() const { return return_value_type_; }

  /**
   * @return number of children in this abstract expression
   */
  size_t GetChildrenSize() const { return children_.size(); }

  /**
   * @param index index of child
   * @return child of abstract expression at that index
   */
  std::shared_ptr<AbstractExpression> GetChild(size_t index) const {
    TERRIER_ASSERT(index < children_.size(), "Index must be in bounds.");
    return children_[index];
  }

 private:
  const ExpressionType expression_type_;                       // type of current expression
  const type::TypeId return_value_type_;                       // type of return value
  std::vector<std::shared_ptr<AbstractExpression>> children_;  // list of children
};

}  // namespace parser
}  // namespace terrier
