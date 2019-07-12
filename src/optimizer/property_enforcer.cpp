#include <vector>

#include "optimizer/property_enforcer.h"
#include "optimizer/physical_operators.h"
#include "optimizer/property.h"
#include "optimizer/properties.h"

namespace terrier::optimizer {

GroupExpression* PropertyEnforcer::EnforceProperty(GroupExpression* gexpr, Property* property) {
  input_gexpr_ = gexpr;
  property->Accept(this);
  return output_gexpr_;
}

void PropertyEnforcer::Visit(const PropertySort *prop) {
  std::vector<GroupID> child_groups(1, input_gexpr_->GetGroupID());
  output_gexpr_ = new GroupExpression(OrderBy::make(), std::move(child_groups));
}

}  // namespace terrier::optimizer
