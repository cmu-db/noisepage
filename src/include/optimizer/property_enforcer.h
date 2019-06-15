#pragma once

#include "optimizer/group_expression.h"
#include "optimizer/operator_expression.h"
#include "optimizer/property_visitor.h"

namespace terrier {
namespace optimizer {

class PropertySet;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Enforce missing physical properties to group expression
class PropertyEnforcer : public PropertyVisitor {
 public:
  /**
   * Enforces a property for a given GroupExpression
   * @param gexpr GroupExpression to enforce the property for
   * @param property Property to enforce
   * @returns GroupExpression that enforces this property
   */
  GroupExpression* EnforceProperty(GroupExpression* gexpr, Property* property);

  /**
   * Implementation of the Visit function for PropertySort
   */
  virtual void Visit(const PropertySort *) override;

 private:
  GroupExpression* input_gexpr_;
  GroupExpression* output_gexpr_;
};

}  // namespace optimizer
}  // namespace terrier
