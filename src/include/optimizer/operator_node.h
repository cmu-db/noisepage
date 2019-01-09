#pragma once

#include "optimizer/property_set.h"
#include "common/hash_util.h"

#include <memory>
#include <string>

namespace terrier::sql {

enum class OpType {
  Undefined = 0,
  DummyScan, /* Dummy Physical Op for SELECT without FROM*/
  SeqScan,
  IndexScan,
  ExternalFileScan,
  QueryDerivedScan,
  OrderBy,
  PhysicalLimit,
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
  BaseOperatorNode() {}

  virtual ~BaseOperatorNode() {}

  virtual void Accept(OperatorVisitor *v) const = 0;

  virtual std::string GetName() const = 0;

  virtual OpType GetType() const = 0;

  virtual std::vector<PropertySet> RequiredInputProperties() const {
    return {};
  }

  virtual common::hash_t Hash() const {
    OpType t = GetType();
    return common::HashUtil::Hash(&t);
  }

  virtual bool operator==(const BaseOperatorNode &r) {
    return GetType() == r.GetType();
  }
};

// Curiously recurring template pattern
template <typename T>
struct OperatorNode : public BaseOperatorNode {
  void Accept(OperatorVisitor *v) const;

  std::string GetName() const { return name_; }

  OpType GetType() const { return type_; }

  static std::string name_;

  static OpType type_;
};

class Operator {
 public:
  Operator();

  Operator(BaseOperatorNode *node);

  // Calls corresponding visitor to node
  void Accept(OperatorVisitor *v) const;

  // Return name of operator
  std::string GetName() const;

  // Return operator type
  OpType GetType() const;

  common::hash_t Hash() const;

  bool operator==(const Operator &r);

  template <typename T>
  const T *As() const {
    if (node && typeid(*node) == typeid(T)) {
      return (const T *)node.get();
    }
    return nullptr;
  }

 private:
  std::shared_ptr<BaseOperatorNode> node;
};
}  // namespace terrier::sql

namespace std {

template <>
struct hash<peloton::optimizer::BaseOperatorNode> {
  typedef peloton::optimizer::BaseOperatorNode argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
