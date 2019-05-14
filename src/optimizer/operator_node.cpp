#include "optimizer/operator_node.h"
#include <string>

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
Operator::Operator() noexcept : node_(nullptr) {}

Operator::Operator(BaseOperatorNode *node) : node_(node) {}

void Operator::Accept(OperatorVisitor *v) const { node_->Accept(v); }

OpType Operator::GetType() const {
  if (IsDefined()) {
    return node_->GetType();
  }
  return OpType::Undefined;
}

common::hash_t Operator::Hash() const {
  if (IsDefined()) {
    return node_->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &r) {
  if (IsDefined() && r.IsDefined()) {
    return *node_ == *r.node_;
  }

  return !IsDefined() && !r.IsDefined();
}

bool Operator::IsDefined() const { return node_ != nullptr; }

}  // namespace terrier::optimizer
