#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "common/json.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"
#include "type/value.h"

namespace terrier::parser {
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
  AbstractExpression(const AbstractExpression &other) = default;

 public:
  virtual ~AbstractExpression() = default;

  /**
   * Hashes the current abstract expression.
   */
  virtual common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(expression_type_);
    for (auto const &child : children_) {
      hash = common::HashUtil::CombineHashes(hash, child->Hash());
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
   * @return true if the two expressions are logically equal
   */
  virtual bool operator!=(const AbstractExpression &rhs) const { return !operator==(rhs); }

  /**
   * Creates a (shallow) copy of the current AbstractExpression.
   */
  // It is incorrect to supply a default implementation here since that will return an object
  // of base type AbstractExpression instead of the desired non-abstract type.
  virtual std::unique_ptr<AbstractExpression> Copy() const = 0;

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
  std::shared_ptr<AbstractExpression> GetChild(uint64_t index) const {
    TERRIER_ASSERT(index < children_.size(), "Index must be in bounds.");
    return children_[index];
  }

  /**
   * Exact equality check, including value equality
   * @param other
   * @return true if the two expressions are exactly equal
   */
  bool ExactlyEquals(const AbstractExpression &other) const {
    if (expression_type_ != other.expression_type_ || children_.size() != other.children_.size()) return false;
    for (unsigned i = 0; i < children_.size(); i++) {
      if (!children_[i]->ExactlyEquals(*other.children_[i].get())) return false;
    }
    return true;
  }

 private:
  const ExpressionType expression_type_;                       // type of current expression
  const type::TypeId return_value_type_;                       // type of return value
  std::vector<std::shared_ptr<AbstractExpression>> children_;  // list of children
};
}  // namespace terrier::parser

namespace std {
/**
 * Implements std::hash for abstract expressions
 */
template <>
struct hash<terrier::parser::AbstractExpression> {
  /**
   * Hashes the given expression
   * @param expr the expression to hash
   * @return hash code of the given expression
   */
  size_t operator()(const terrier::parser::AbstractExpression &expr) const { return expr.Hash(); }
};
}  // namespace std
