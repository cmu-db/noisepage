#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "common/json.h"
#include "common/sql_node_visitor.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"
#include "type/type_id.h"

namespace terrier::parser {
/**
 * AbstractExpression is the base class of any expression which is output from the parser.
 *
 * TODO(WAN): So these are supposed to be dumb and immutable, but we're cheating in the binder. Figure out how to
 * document these assumptions exactly.
 */
class AbstractExpression {
 protected:
  /**
   * Instantiates a new abstract expression.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param children the list of children for this node
   */
  AbstractExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : expression_type_(expression_type), return_value_type_(return_value_type), children_(std::move(children)) {}
  /**
   * Instantiates a new abstract expression with alias used for select statement column references.
   * @param expression_type what type of expression we have
   * @param return_value_type the type of the expression's value
   * @param alias alias of the column (used in column value expression)
   * @param children the list of children for this node
   */
  AbstractExpression(const ExpressionType expression_type, const type::TypeId return_value_type, std::string alias,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : expression_type_(expression_type),
        alias_(std::move(alias)),
        return_value_type_(return_value_type),
        children_(std::move(children)) {}

  /**
   * Copy constructs an abstract expression.
   * @param other the abstract expression to be copied
   */
  AbstractExpression(const AbstractExpression &other) = default;

  /**
   * Default constructor used for json deserialization
   */
  AbstractExpression() = default;

  /**
   * @param expression_name Set the expression name of the current expression
   */
  void SetExpressionName(std::string expression_name) { expression_name_ = std::move(expression_name); }

  /**
   * @param return_value_type Set the return value type of the current expression
   */
  void SetReturnValueType(type::TypeId return_value_type) { return_value_type_ = return_value_type; }

  /**
   * @param depth Set the depth of the current expression
   */
  void SetDepth(int depth) { depth_ = depth; }

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
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(return_value_type_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(expression_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(alias_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(depth_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(has_subquery_));

    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  virtual bool operator==(const AbstractExpression &rhs) const {
    if (expression_type_ != rhs.expression_type_) return false;
    if (alias_ != rhs.alias_) return false;
    if (expression_name_ != rhs.expression_name_) return false;
    if (depth_ != rhs.depth_) return false;
    if (has_subquery_ != rhs.has_subquery_) return false;
    if (children_.size() != rhs.children_.size()) return false;
    for (size_t i = 0; i < children_.size(); i++)
      if (*(children_[i]) != *(rhs.children_[i])) return false;
    return return_value_type_ == rhs.return_value_type_;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two expressions are logically not equal
   */
  virtual bool operator!=(const AbstractExpression &rhs) const { return !operator==(rhs); }

  /**
   * Creates a deep copy of the current AbstractExpression.
   *
   * Transplanted comment from subquery_expression.h,
   * TODO(WAN): Previous codebase described as a hack, will we need a deep copy?
   * Tianyu: No need for deep copy if your objects are always immutable! (why even copy at all, but that's beyond me)
   * Wan: objects are becoming mutable again..
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

  /** @return children of this abstract expression */
  const std::vector<std::unique_ptr<AbstractExpression>> &GetChildren() const { return children_; }

  /**
   * @param index index of child
   * @return child of abstract expression at that index
   */
  common::ManagedPointer<AbstractExpression> GetChild(uint64_t index) const {
    TERRIER_ASSERT(index < children_.size(), "Index must be in bounds.");
    return common::ManagedPointer(children_[index]);
  }

  /** @return Name of the expression. */
  const std::string &GetExpressionName() const { return expression_name_; }

  /**
   * Walks the expression trees and generate the correct expression name
   */
  virtual void DeriveExpressionName();

  /** @return alias of this abstract expression */
  const std::string &GetAlias() const { return alias_; }

  /**
   * Derive the expression type of the current expression.
   */
  virtual void DeriveReturnValueType() {}

  // TODO(WAN): it looks like we are returning to Peloton style Accept calls AcceptChildren calls Accept, must we?

  /** @param v Visitor pattern for the expression */
  virtual void Accept(SqlNodeVisitor *v) = 0;

  /** @param v Visitor pattern for the expression */
  virtual void AcceptChildren(SqlNodeVisitor *v) {
    for (auto &child : children_) child->Accept(v);
  }

  /** @return the sub-query depth level (SEE COMMENT in depth_) */
  int GetDepth() const { return depth_; }

  /**
   * Derive the sub-query depth level of the current expression
   * @return the derived depth (SEE COMMENT in depth_)
   */
  virtual int DeriveDepth();

  /** @return true iff the current expression contains a subquery
   * DeriveSubqueryFlag() MUST be called between modifications to this expression or its children for this
   * function to return a meaningful value.
   * */
  bool HasSubquery() const { return has_subquery_; }

  /**
   * Derive if there's sub-query in the current expression
   * @return true iff the current expression contains a subquery
   */
  virtual bool DeriveSubqueryFlag();

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
  /** Type of the current expression */
  ExpressionType expression_type_;
  /** Name of the current expression */
  std::string expression_name_;
  /** Alias of the current expression */
  std::string alias_;
  /** Type of the return value */
  type::TypeId return_value_type_;

  /**
   * MUTABLE Sub-query depth level for the current expression.
   *
   * TODO(WAN): ask LING to document her assumptions, I see that depth can still be -1 after calling DeriveDepth().
   *
   * DeriveDepth() MUST be called on this expression tree whenever the structure of the tree is modified.
   * -1 indicates that the depth has not been set, but we have no safeguard for maintaining accurate depths between
   * tree modifications.
   */
  int depth_ = -1;
  /**
   * MUTABLE Flag indicating if there's a sub-query in the current expression or in any of its children.
   *
   * TODO(WAN): check with LING on why we need this and whether we can roll DeriveSubqueryFlag into DeriveDepth.
   * */
  bool has_subquery_ = false;

  /** List of children expressions. */
  std::vector<std::unique_ptr<AbstractExpression>> children_;
};

DEFINE_JSON_DECLARATIONS(AbstractExpression)

/**
 * DeserializeExpression is the primary function used to deserialize arbitrary expressions.
 * It will switch on the type in the JSON object to construct the appropriate expression.
 * @param json json to deserialize
 * @return pointer to deserialized expression
 */
std::unique_ptr<AbstractExpression> DeserializeExpression(const nlohmann::json &j);

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
