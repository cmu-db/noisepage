#include "optimizer/property_enforcer.h"

#include <utility>
#include <vector>

#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/property.h"

namespace noisepage::optimizer {

GroupExpression *PropertyEnforcer::EnforceProperty(GroupExpression *gexpr, Property *property,
                                                   transaction::TransactionContext *txn) {
  input_gexpr_ = gexpr;
  txn_ = txn;
  property->Accept(this);
  return output_gexpr_;
}

void PropertyEnforcer::Visit(const PropertySort *prop) {
  std::vector<group_id_t> child_groups(1, input_gexpr_->GetGroupID());
  output_gexpr_ = new GroupExpression(OrderBy::Make().RegisterWithTxnContext(txn_), std::move(child_groups), txn_);
}

}  // namespace noisepage::optimizer
