#include "optimizer/operator_node_contents.h"

#include <memory>
#include <string>
#include <utility>

#include "common/managed_pointer.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

Operator::Operator() noexcept = default;

Operator::Operator(common::ManagedPointer<BaseOperatorNodeContents> contents)
    : AbstractOptimizerNodeContents(contents.CastManagedPointerTo<AbstractOptimizerNodeContents>()) {}

Operator::Operator(Operator &&o) noexcept : AbstractOptimizerNodeContents(o.contents_) {}

void Operator::Accept(common::ManagedPointer<OperatorVisitor> v) const { contents_->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return contents_->GetName();
  }
  return "Undefined";
}

OpType Operator::GetOpType() const {
  if (IsDefined()) {
    return contents_->GetOpType();
  }

  return OpType::UNDEFINED;
}

parser::ExpressionType Operator::GetExpType() const { return parser::ExpressionType::INVALID; }

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

Operator Operator::RegisterWithTxnContext(transaction::TransactionContext *txn) {
  auto *op = dynamic_cast<BaseOperatorNodeContents *>(contents_.Get());
  if (txn != nullptr) {
    txn->RegisterCommitAction([=]() { delete op; });
    txn->RegisterAbortAction([=]() { delete op; });
  }
  return *this;
}

}  // namespace noisepage::optimizer
