#include <type_traits>
#include <vector>

#include "optimizer/group_expression.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/physical_operators.h"
#include "optimizer/property.h"
#include "optimizer/property_enforcer.h"

namespace terrier::optimizer {
class PropertySort;
}  // namespace terrier::optimizer

namespace terrier::optimizer {

GroupExpression *PropertyEnforcer::EnforceProperty(GroupExpression *gexpr, Property *property) {
  input_gexpr_ = gexpr;
  property->Accept(this);
  return output_gexpr_;
}

void PropertyEnforcer::Visit(const PropertySort *prop) {
  std::vector<group_id_t> child_groups(1, input_gexpr_->GetGroupID());
  output_gexpr_ = new GroupExpression(OrderBy::Make(), std::move(child_groups));
}

}  // namespace terrier::optimizer
