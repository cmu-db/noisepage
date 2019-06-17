#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "common/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"
#include "type/type_id.h"

namespace terrier::parser {
/**
 * An abstract parser expression. Dumb and immutable.
 */
class AbstractExpression {
 //friend class BindNodeVisitor;
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

  /**
   * Default constructor used for json deserialization
   */
  AbstractExpression() = default;

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
   * @return true if the two expressions are logically not equal
   */
  virtual bool operator!=(const AbstractExpression &rhs) const { return !operator==(rhs); }

  /**
   * Creates a (shallow) copy of the current AbstractExpression.
   */
  // It is incorrect to supply a default implementation here since that will return an object
  // of base type AbstractExpression instead of the desired non-abstract type.
  virtual std::shared_ptr<AbstractExpression> Copy() const = 0;

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
   * @return children of this abstract expression
   */
  const std::vector<std::shared_ptr<AbstractExpression>> &GetChildren() const { return children_; }

  /**
   * @param index index of child
   * @return child of abstract expression at that index
   */
  std::shared_ptr<AbstractExpression> GetChild(uint64_t index) const {
    TERRIER_ASSERT(index < children_.size(), "Index must be in bounds.");
    return children_[index];
  }

  const std::string &GetExpressionName() const { return expr_name_; }

  /**
   * @return alias of this abstract expression
   */
  const std::string &GetAlias() const { return alias_; }

  /**
   * Set the alias of the current expression
   *
   * @param alias The alias of this abstract expression
   */
  void SetAlias(std::string alias) { alias_ = alias; }

  /**
   * Deduce the expression type of the current expression.
   */
  virtual void DeduceExpressionType() {}

  virtual void Accept(SqlNodeVisitor *) = 0;

  virtual void AcceptChildren(SqlNodeVisitor *v) {
    for (auto &child : children_) {
      child->Accept(v);
    }
  }

  /**
   * @return The sub-query depth level
   */
  int GetDepth() const { return depth_; }

  /**
   * @return The sub-query flag that tells if the current expression contain a sub-query
   */
  bool HasSubquery() const { return has_subquery_; }

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

 private:

  /**
   * @brief Derive if there's sub-query in the current expression
   *
   * @return If there is sub-query, then return true, otherwise return false
   */

  virtual bool DeriveSubqueryFlag() {
    if (expression_type_ == ExpressionType::ROW_SUBQUERY || expression_type_ == ExpressionType::SELECT_SUBQUERY) {
      has_subquery_ = true;
    } else {
      for (auto &child : children_) {
        if (child->DeriveSubqueryFlag()) {
          has_subquery_ = true;
          break;
        }
      }
    }
    return has_subquery_;
  }

  /**
   * @brief Derive the sub-query depth level of the current expression
   *
   * @return the derived depth
   */
  virtual int DeriveDepth() {
    if (depth_ < 0) {
      for (auto &child : children_) {
        auto child_depth = child->DeriveDepth();
        if (child_depth >= 0 && (depth_ == -1 || child_depth < depth_))
          depth_ = child_depth;
      }
    }
    return depth_;
  }

  /**
   * Walks the expression trees and generate the correct expression name
   */
  virtual void DeduceExpressionName();

  /**
   * Set the sub-query depth level of the current expression
   *
   * @param depth The depth to set
   */
  void SetDepth(int depth) { depth_ = depth; }

  /**
   * Type of the current expression
   */
  ExpressionType expression_type_;
  /**
   * Type of the return value
   */
  type::TypeId return_value_type_;
  /**
   * List fo children expressions
   */
  std::vector<std::shared_ptr<AbstractExpression>> children_;
  /**
   * The current sub-query depth level in the current expression, -1
   *  stands for not derived
   */
  int depth_ = -1;
  /**
   * The flag indicating if there's sub-query in the current expression
   */
  bool has_subquery_ = false;
  /**
   * Name of the current expression
   */
  std::string expr_name_;
  /**
   * Alias of the current expression
   */
  std::string alias_;
};

DEFINE_JSON_DECLARATIONS(AbstractExpression);

/**
 * This should be the primary function used to deserialize an expression
 * @param json json to deserialize
 * @return pointer to deserialized expression
 */
std::shared_ptr<AbstractExpression> DeserializeExpression(const nlohmann::json &j);

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
