#include "optimizer/operator_node.h"
#include <memory>
#include <string>
#include <utility>
#include "common/managed_pointer.h"

namespace terrier::optimizer {

Operator::Operator() noexcept : node_(nullptr) {}

Operator::Operator(std::unique_ptr<BaseOperatorNode> node) : node_(std::move(node)) {}

Operator::Operator(Operator &&o) noexcept : node_(std::move(o.node_)) {}

void Operator::Accept(common::ManagedPointer<OperatorVisitor> v) const { node_->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return node_->GetName();
  }
  return "Undefined";
}

OpType Operator::GetType() const {
  if (IsDefined()) {
    return node_->GetType();
  }
  return OpType::UNDEFINED;
}

bool Operator::IsLogical() const {
  if (IsDefined()) {
    return node_->IsLogical();
  }
  return false;
}

bool Operator::IsPhysical() const {
  if (IsDefined()) {
    return node_->IsPhysical();
  }
  return false;
}

common::hash_t Operator::Hash() const {
  if (IsDefined()) {
    return node_->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &rhs) {
  if (IsDefined() && rhs.IsDefined()) {
    return *node_ == *rhs.node_;
  }

  return !IsDefined() && !rhs.IsDefined();
}

bool Operator::IsDefined() const { return node_ != nullptr; }

}  // namespace terrier::optimizer
