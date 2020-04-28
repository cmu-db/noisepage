#include "optimizer/operator_node_contents.h"

#include <memory>
#include <type_traits>

#include "common/managed_pointer.h"

namespace terrier::optimizer {

Operator::Operator() noexcept = default;

Operator::Operator(std::unique_ptr<BaseOperatorNodeContents> contents) : contents_(std::move(contents)) {}

Operator::Operator(Operator &&o) noexcept : contents_(std::move(o.contents_)) {}

void Operator::Accept(common::ManagedPointer<OperatorVisitor> v) const { contents_->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return contents_->GetName();
  }
  return "Undefined";
}

OpType Operator::GetType() const {
  if (IsDefined()) {
    return contents_->GetType();
  }

  return OpType::UNDEFINED;
}

bool Operator::IsLogical() const {
  if (IsDefined()) {
    return contents_->IsLogical();
  }
  return false;
}

bool Operator::IsPhysical() const {
  if (IsDefined()) {
    return contents_->IsPhysical();
  }
  return false;
}

common::hash_t Operator::Hash() const {
  if (IsDefined()) {
    return contents_->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &rhs) const {
  if (IsDefined() && rhs.IsDefined()) {
    return *contents_ == *rhs.contents_;
  }

  return !IsDefined() && !rhs.IsDefined();
}

bool Operator::IsDefined() const { return contents_ != nullptr; }

}  // namespace terrier::optimizer
