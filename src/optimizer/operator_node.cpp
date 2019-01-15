#include "optimizer/operator_node.h"

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
Operator::Operator() : node(nullptr) {}

Operator::Operator(BaseOperatorNode *node) : node(node) {}

void Operator::Accept(OperatorVisitor *v) const { node->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return node->GetName();
  }
  return "Undefined";
}

OpType Operator::GetType() const {
  if (IsDefined()) {
    return node->GetType();
  }
  return OpType::Undefined;
}

common::hash_t Operator::Hash() const {
  if (IsDefined()) {
    return node->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &r) {
  if (IsDefined() && r.IsDefined()) {
    return *node == *r.node;
  }

  return !IsDefined() && !r.IsDefined();
}

bool Operator::IsDefined() const { return node != nullptr; }

}  // namespace terrier::optimizer
