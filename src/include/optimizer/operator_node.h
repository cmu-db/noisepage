#pragma once

#include <memory>
#include <string>
#include <vector>
#include "common/hash_util.h"
#include "optimizer/property_set.h"
#include "optimizer_defs.h"

namespace terrier::optimizer {

/**
 * Utility class for visiting the operator tree
 */
class OperatorVisitor;

struct BaseOperatorNode {
  BaseOperatorNode() = default;

  virtual ~BaseOperatorNode() = default;

  virtual void Accept(OperatorVisitor *v) const = 0;

  virtual std::string GetName() const = 0;

  virtual OpType GetType() const = 0;

  virtual std::vector<PropertySet> RequiredInputProperties() const { return {}; }

  virtual common::hash_t Hash() const {
    OpType t = GetType();
    return common::HashUtil::Hash(&t);
  }

  virtual bool operator==(const BaseOperatorNode &r) { return GetType() == r.GetType(); }
};

template <typename T>
struct OperatorNode : public BaseOperatorNode {
  void Accept(OperatorVisitor *v) const override;

  std::string GetName() const override { return std::string(name_); }

  OpType GetType() const override { return type_; }

  static const char *name_;

  static OpType type_;
};

/**
 * Logical and physical operators
 */
class Operator {
 public:

  /**
   * Default constructor
   */
  Operator() = default;

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
   * Re-interpret the operator
   * @tparam T the type of the operator to be re-interpretted as
   * @return pointer to the re-interpretted operator, nullptr if the types mismatch
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
  using argument_type = terrier::optimizer::BaseOperatorNode;
  using result_type = std::size_t;
  /**
   * std::hash operator for BaseOperatorNode
   * @param s a BaseOperatorNode
   * @return hashed value
   */
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
