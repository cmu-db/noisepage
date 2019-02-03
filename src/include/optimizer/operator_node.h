#pragma once

#include <memory>
#include <string>
#include <vector>
#include "common/hash_util.h"
#include "optimizer/property_set.h"

namespace terrier::optimizer {

enum class OpType {
  Undefined = 0,
  DummyScan, /* Dummy Op for SELECT without FROM*/
  SeqScan,
  IndexScan,
  ExternalFileScan,
  QueryDerivedScan,
  OrderBy,
  Limit,
  Distinct,
  InnerNLJoin,
  LeftNLJoin,
  RightNLJoin,
  OuterNLJoin,
  InnerHashJoin,
  LeftHashJoin,
  RightHashJoin,
  OuterHashJoin,
  Insert,
  InsertSelect,
  Delete,
  Update,
  Aggregate,
  HashGroupBy,
  SortGroupBy,
  ExportExternalFile,
};

//===--------------------------------------------------------------------===//
// Operator Node
//===--------------------------------------------------------------------===//
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

class Operator {
 public:
  Operator();

  explicit Operator(BaseOperatorNode *node);

  // Calls corresponding visitor to node
  void Accept(OperatorVisitor *v) const;

  // Return name of operator
  std::string GetName() const;

  // Return operator type
  OpType GetType() const;

  common::hash_t Hash() const;

  bool operator==(const Operator &r);

  bool IsDefined() const;

  template <typename T>
  const T *As() const {
    if (node) {
      auto &n = *node;
      if (typeid(n) == typeid(T)) {
        return reinterpret_cast<const T *>(node.get());
      }
    }
    return nullptr;
  }

 private:
  std::shared_ptr<BaseOperatorNode> node;
};
}  // namespace terrier::optimizer

namespace std {

template <>
struct hash<terrier::optimizer::BaseOperatorNode> {
  using argument_type = terrier::optimizer::BaseOperatorNode;
  using result_type = std::size_t;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
