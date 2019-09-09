#pragma once

#include "optimizer/group_expression.h"
#include "optimizer/operator_expression.h"
#include "optimizer/property_visitor.h"

namespace terrier::optimizer {

class PropertySet;

/**
 * PropertyEnforcer class is used for enforcing properties for
 * a specified GroupExpression.
 */
class PropertyEnforcer : public PropertyVisitor {
 public:
  /**
   * Enforces a property for a given GroupExpression
   * @param gexpr GroupExpression to enforce the property for
   * @param property Property to enforce
   * @returns GroupExpression that enforces this property
   */
  GroupExpression *EnforceProperty(GroupExpression *gexpr, Property *property);

  /**
   * Implementation of the Visit function for PropertySort
   * @param prop PropertySort being visited
   */
  void Visit(const PropertySort *prop) override;

 private:
  /**
   * Input GroupExpression to enforce
   */
  GroupExpression *input_gexpr_;

  /**
   * Output GroupExpression after enforcing
   */
  GroupExpression *output_gexpr_;
};

}  // namespace terrier::optimizer
