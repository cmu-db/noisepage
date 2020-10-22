#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node_contents.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimizer_defs.h"

namespace terrier::transaction {
class TransactionContext;
}  // namespace terrier::transaction

namespace terrier::optimizer {

/**
 * Base class for operators
 */
class BaseOperatorNodeContents : public AbstractOptimizerNodeContents {
 public:
  /**
   * Default constructor
   */
  BaseOperatorNodeContents() = default;

  /**
   * Default destructor
   */
  ~BaseOperatorNodeContents() override = default;

  /**
   * Copy
   * @returns copy of this
   */
  virtual BaseOperatorNodeContents *Copy() const = 0;

  /**
   * Utility method for visitor pattern
   * @param v operator visitor for visitor pattern
   */
  void Accept(common::ManagedPointer<OperatorVisitor> v) const override = 0;

  /**
   * @return the string name of this operator
   */
  std::string GetName() const override = 0;

  /**
   * @return the type of this operator
   */
  OpType GetOpType() const override = 0;

  /**
   * @return the ExpressionType of this operator (invalid expression type)
   */
  parser::ExpressionType GetExpType() const override = 0;

  /**
   * @return whether this operator is logical
   */
  bool IsLogical() const override = 0;

  /**
   * @return whether this operator is physical
   */
  bool IsPhysical() const override = 0;

  /**
   * @return the hashed value of this operator
   */
  common::hash_t Hash() const override {
    OpType t = GetOpType();
    return common::HashUtil::Hash(t);
  }

  /**
   * Equality check
   * @param r The other (abstract) node contents
   * @return true if this operator is logically equal to the other, false otherwise
   */
  bool operator==(const AbstractOptimizerNodeContents &r) override {
    if (r.GetOpType() != OpType::UNDEFINED) {
      const auto &contents = dynamic_cast<const BaseOperatorNodeContents &>(r);
      return (*this == contents);
    }
    return false;
  }

  /**
   * Inequality check
   * @param r The other (abstract) node contents
   * @return false if this operator is logically equal to the other, true otherwise
   */
  bool operator!=(const AbstractOptimizerNodeContents &r) { return !(operator==(r)); }

  /**
   * Equality check
   * @param r other
   * @return true if this operator is logically equal to other, false otherwise
   */
  virtual bool operator==(const BaseOperatorNodeContents &r) { return GetOpType() == r.GetOpType(); }

  /**
   * Inequality check
   * @param r other
   * @return true if this operator is logically not equal to other, false otherwise
   */
  virtual bool operator!=(const BaseOperatorNodeContents &r) { return !operator==(r); }
};

/**
 * A wrapper around operators to provide a universal interface for accessing the data within
 * @tparam T an operator type
 */
template <typename T>
class OperatorNodeContents : public BaseOperatorNodeContents {
 protected:
  /**
   * Utility method for applying visitor pattern on the underlying operator
   * @param v operator visitor for visitor pattern
   */
  void Accept(common::ManagedPointer<OperatorVisitor> v) const override { v->Visit(reinterpret_cast<const T *>(this)); }

  /**
   * Copy
   * @returns copy of this
   */
  BaseOperatorNodeContents *Copy() const override = 0;

  /**
   * @return string name of the underlying operator
   */
  std::string GetName() const override { return std::string(name); }

  /**
   * @return type of the underlying operator
   */
  OpType GetOpType() const override { return type; }

  /**
   * @return ExpressionType of the operator (invalid expression type)
   */
  parser::ExpressionType GetExpType() const override { return parser::ExpressionType::INVALID; }

  /**
   * @return whether the underlying operator is logical
   */
  bool IsLogical() const override { return type < OpType::LOGICALPHYSICALDELIMITER; }

  /**
   * @return whether the underlying operator is physical
   */
  bool IsPhysical() const override { return type > OpType::LOGICALPHYSICALDELIMITER; }

 private:
  /**
   * Name of the operator
   */
  static const char *name;

  /**
   * Type of the operator
   */
  static OpType type;
};

/**
 * Logical and physical operators
 */
class Operator : public AbstractOptimizerNodeContents {
 public:
  /**
   * Default constructor
   */
  Operator() noexcept;

  /**
   * Create a new operator from a BaseOperatorNodeContents
   * @param contents a BaseOperatorNodeContents that specifies basic info about the operator to be created
   */
  explicit Operator(common::ManagedPointer<BaseOperatorNodeContents> contents);

  /**
   * Move constructor
   * @param o other to construct from
   */
  Operator(Operator &&o) noexcept;

  /**
   * Copy constructor for Operator
   */
  Operator(const Operator &op) : AbstractOptimizerNodeContents(op.contents_) {}

  /**
   * Default destructor
   */
  ~Operator() override = default;

  /**
   * Calls corresponding visitor to this operator node
   */
  void Accept(common::ManagedPointer<OperatorVisitor> v) const override;

  /**
   * @return string name of this operator
   */
  std::string GetName() const override;

  /**
   * @return type of this operator
   */
  OpType GetOpType() const override;

  /**
   * @return ExpressionType for operator (Invalid expression type)
   */
  parser::ExpressionType GetExpType() const override;

  /**
   * @return hashed value of this operator
   */
  common::hash_t Hash() const override;

  /**
   * Logical equality check
   * @param rhs other
   * @return true if the two operators are logically equal, false otherwise
   */
  bool operator==(const Operator &rhs) const;

  /**
   * Logical inequality check
   * @param rhs other
   * @return true if the two operators are logically not equal, false otherwise
   */
  bool operator!=(const Operator &rhs) const { return !operator==(rhs); }

  /**
   * @return true if the operator is defined, false otherwise
   */
  bool IsDefined() const override;

  /**
   * @return true if the operator is logical, false otherwise
   */
  bool IsLogical() const override;

  /**
   * @return true if the operator is physical, false otherwise
   */
  bool IsPhysical() const override;

  /**
   * Registers the operator with the provided transaction context, so that
   *   its memory will be freed when the transaction commits or aborts.
   * @param txn the transaction context it's registered to
   * @return this operator (for easy chaining)
   */
  Operator RegisterWithTxnContext(transaction::TransactionContext *txn);
};
}  // namespace terrier::optimizer

namespace std {

/**
 * Hash function object of a BaseOperatorNodeContents
 */
template <>
struct hash<terrier::optimizer::BaseOperatorNodeContents> {
  /**
   * Argument type of the base operator
   */
  using argument_type = terrier::optimizer::BaseOperatorNodeContents;

  /**
   * Result type of the base operator
   */
  using result_type = std::size_t;

  /**
   * std::hash operator for BaseOperatorNodeContents
   * @param s a BaseOperatorNodeContents
   * @return hashed value
   */
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
