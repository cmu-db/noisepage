#pragma once

#include <memory>
#include <string>
#include <vector>
#include "common/hash_util.h"
#include "optimizer/optimizer_defs.h"

namespace terrier::optimizer {

/**
 * Utility class for visiting the operator tree
 */
class OperatorVisitor;

/**
 * Base class for operators
 */
class BaseOperatorNode {
 public:
  /**
   * Default constructor
   */
  BaseOperatorNode() = default;

  /**
   * Default destructor
   */
  virtual ~BaseOperatorNode() = default;

  /**
   * Utility method for visitor pattern
   * @param v operator visitor for visitor pattern
   */
  virtual void Accept(OperatorVisitor *v) const = 0;

  /**
   * @return the string name of this operator
   */
  virtual std::string GetName() const = 0;

  /**
   * @return the type of this operator
   */
  virtual OpType GetType() const = 0;

  /**
   * @return whether this operator is logical
   */
  virtual bool IsLogical() const = 0;

  /**
   * @return whether this operator is physical
   */
  virtual bool IsPhysical() const = 0;

  /**
   * @return the hashed value of this operator
   */
  virtual common::hash_t Hash() const {
    OpType t = GetType();
    return common::HashUtil::Hash(t);
  }

  /**
   * Equality check
   * @param r other
   * @return true if this operator is logically equal to other, false otherwise
   */
  virtual bool operator==(const BaseOperatorNode &r) { return GetType() == r.GetType(); }

  /**
   * Inequality check
   * @param r other
   * @return true if this operator is logically not equal to other, false otherwise
   */
  virtual bool operator!=(const BaseOperatorNode &r) { return !operator==(r); }
};

/**
 * A wrapper around operators to provide a universal interface for accessing the data within
 * @tparam T an operator type
 */
template <typename T>
class OperatorNode : public BaseOperatorNode {
 protected:
  /**
   * Utility method for applying visitor pattern on the underlying operator
   * @param v operator visitor for visitor pattern
   */
  void Accept(OperatorVisitor *v) const override;

  /**
   * @return string name of the underlying operator
   */
  std::string GetName() const override { return std::string(name); }

  /**
   * @return type of the underlying operator
   */
  OpType GetType() const override { return type; }

  /**
   * @return whether the underlying operator is logical
   */
  bool IsLogical() const override;

  /**
   * @return whether the underlying operator is physical
   */
  bool IsPhysical() const override;

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
class Operator {
 public:
  /**
   * Default constructor
   */
  Operator() noexcept;

  /**
   * Create a new operator from a BaseOperatorNode
   * @param node a BaseOperatorNode that specifies basic information about the operator to be created
   */
  explicit Operator(BaseOperatorNode *node);

  /**
   * Calls corresponding visitor to this operator node
   */
  void Accept(OperatorVisitor *v) const;

  /**
   * @return string name of this operator
   */
  std::string GetName() const;

  /**
   * @return type of this operator
   */
  OpType GetType() const;

  /**
   * @return hashed value of this operator
   */
  common::hash_t Hash() const;

  /**
   * Logical equality check
   * @param rhs other
   * @return true if the two operators are logically equal, false otherwise
   */
  bool operator==(const Operator &rhs);

  /**
   * Logical inequality check
   * @param rhs other
   * @return true if the two operators are logically not equal, false otherwise
   */
  bool operator!=(const Operator &rhs) { return !operator==(rhs); }

  /**
   * @return true if the operator is defined, false otherwise
   */
  bool IsDefined() const;

  /**
   * @return true if the operator is logical, false otherwise
   */
  bool IsLogical() const;

  /**
   * @return true if the operator is physical, false otherwise
   */
  bool IsPhysical() const;

  /**
   * default destructor, the content of the Operator should be explicitly managed
   */
  ~Operator() = default;

  /**
   * Re-interpret the operator
   * @tparam T the type of the operator to be re-interpreted as
   * @return pointer to the re-interpreted operator, nullptr if the types mismatch
   */
  template <typename T>
  const T *As() const {
    if (node_) {
      auto &n = *node_;
      if (typeid(n) == typeid(T)) {
        return reinterpret_cast<const T *>(node_.get());
      }
    }
    return nullptr;
  }

 private:
  /**
   * Pointer to the base operator
   */
  std::shared_ptr<BaseOperatorNode> node_;
};
}  // namespace terrier::optimizer

namespace std {

/**
 * Hash function object of a BaseOperatorNode
 */
template <>
struct hash<terrier::optimizer::BaseOperatorNode> {
  /**
   * Argument type of the base operator
   */
  using argument_type = terrier::optimizer::BaseOperatorNode;

  /**
   * Result type of the base operator
   */
  using result_type = std::size_t;

  /**
   * std::hash operator for BaseOperatorNode
   * @param s a BaseOperatorNode
   * @return hashed value
   */
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
