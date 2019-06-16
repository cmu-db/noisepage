#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "common/json.h"
#include "common/managed_pointer.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"
#include "type/type_id.h"

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
                     std::vector<const AbstractExpression *> children)
      : expression_type_(expression_type), return_value_type_(return_value_type), children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other) = default;

  /**
   * Default constructor used for json deserialization
   */
  AbstractExpression() = default;

 public:
  virtual ~AbstractExpression() {
    for (auto *child : children_) {
      delete child;
    }
  }

  /**
   * Hashes the current abstract expression.
   */
  virtual common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(expression_type_);
    for (const auto *child : children_) {
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
   * @return true if the two expressions are logically not equal
   */
  virtual bool operator!=(const AbstractExpression &rhs) const { return !operator==(rhs); }

  /**
   * Creates a deep copy of the current AbstractExpression.
   * @warning Should be called sparingly and under careful consideration. A shallow copy will do most times. Consider
   * that AbstractExpression trees can be very large, and a deep copy can be expensive
   * @warning It is incorrect to supply a default implementation here since that will return an object of base type
   * AbstractExpression instead of the desired non-abstract type.
   */
  virtual const AbstractExpression *Copy() const = 0;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   */
  virtual const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const = 0;

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
  common::ManagedPointer<const AbstractExpression> GetChild(uint64_t index) const {
    TERRIER_ASSERT(index < children_.size(), "Index must be in bounds.");
    return common::ManagedPointer<const AbstractExpression>(children_[index]);
  }

  /**
   * Derived expressions should call this base method
   * @return expression serialized to json
   */
  virtual nlohmann::json ToJson() const;

  /**
   * Derived expressions should call this base method
   * @param j json to deserialize
   */
  virtual void FromJson(const nlohmann::json &j);

 protected:
  /**
   * type of current expression
   */
  ExpressionType expression_type_;

  /**
   * type of return value
   */
  type::TypeId return_value_type_;

  /**
   * list of children
   */
  std::vector<const AbstractExpression *> children_;
};

DEFINE_JSON_DECLARATIONS(AbstractExpression);

/**
 * This should be the primary function used to deserialize an expression
 * @param json json to deserialize
 * @return pointer to deserialized expression
 */
AbstractExpression *DeserializeExpression(const nlohmann::json &j);

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
