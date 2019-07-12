#pragma once

#include <memory>
#include <vector>
#include <utility>

#include "common/managed_pointer.h"

#include "parser/parser_defs.h"
#include "parser/expression/abstract_expression.h"

#include "settings/settings_manager.h"

#include "optimizer/property_set.h"
#include "optimizer/operator_expression.h"

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::catalog {
class CatalogAccessor;
}

namespace terrier::transaction {
class TransactionContext;
}

namespace terrier::optimizer {

/**
 * Struct defining information about the query.
 * Struct encapsulates information about the original statement's type,
 * output expressions, and required properties.
 */
struct QueryInfo {
  /**
   * Constructor for QueryInfo
   * @param type StatementType
   * @param exprs Output expressions of the query
   * @param props Physical properties of the output (QueryInfo will own)
   */
  QueryInfo(parser::StatementType type,
            std::vector<common::ManagedPointer<parser::AbstractExpression>> &&exprs,
            PropertySet* props)
      : stmt_type_(type), output_exprs_(exprs), physical_props_(props) {}

  /**
   * Destructor
   */
  ~QueryInfo() {
    delete physical_props_;
  }

  /**
   * @returns StatementType
   */
  parser::StatementType GetStmtType() const { return stmt_type_; }

  /**
   * @returns Output expressions of the query
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> GetOutputExprs() const {
    return output_exprs_;
  }

  /**
   * @returns Physical properties of the output owned by QueryInfo
   */
  PropertySet* GetPhysicalProperties() const {
    return physical_props_;
  }

 private:
  parser::StatementType stmt_type_;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_exprs_;
  PropertySet* physical_props_;
};

/**
 * Class definition for an optimizer.
 * The abstract definition includes only the primary entrypoint
 * `BuildPlanTree` for constructing the optimized plan tree.
 */
class AbstractOptimizer {
 public:
  /**
   * Disallow copy and move
   */
  DISALLOW_COPY_AND_MOVE(AbstractOptimizer);

  AbstractOptimizer() = default;
  virtual ~AbstractOptimizer() = default;

  /**
   * Build the plan tree for query execution
   * @param op_tree Logical operator tree for execution
   * @param query_info Information about the query
   * @param txn TransactionContext
   * @param settings SettingsManager to read settings from
   * @param accessor CatalogAccessor for catalog
   * @returns execution plan
   */
  virtual planner::AbstractPlanNode* BuildPlanTree(
      OperatorExpression* op_tree,
      QueryInfo query_info,
      transaction::TransactionContext *txn,
      settings::SettingsManager *settings,
      catalog::CatalogAccessor *accessor) = 0;

  /**
   * Reset the optimizer's internal state
   */
  virtual void Reset(){}
};

}  // namespace terrier::optimizer
