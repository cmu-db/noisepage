#pragma once

#include "optimizer/group_expression.h"
#include "optimizer/operator_node.h"
#include "optimizer/property_visitor.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

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
   * @param txn transaction context for memory management
   * @returns GroupExpression that enforces this property
   */
  GroupExpression *EnforceProperty(GroupExpression *gexpr, Property *property, transaction::TransactionContext *txn);

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

  /**
   * Transaction context for managing memory
   */
  transaction::TransactionContext *txn_;
};

}  // namespace noisepage::optimizer
