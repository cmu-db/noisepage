#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "optimizer/operator_node.h"
#include "optimizer/property_set.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"

namespace noisepage::planner {
class AbstractPlanNode;
}

namespace noisepage::catalog {
class CatalogAccessor;
}

namespace noisepage::transaction {
class TransactionContext;
}

namespace noisepage::optimizer {

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
   * @param props Physical properties of the output (QueryInfo will not own)
   */
  QueryInfo(parser::StatementType type, std::vector<common::ManagedPointer<parser::AbstractExpression>> &&exprs,
            PropertySet *props)
      : stmt_type_(type), output_exprs_(exprs), physical_props_(props) {}

  /**
   * @returns StatementType
   */
  parser::StatementType GetStatementType() const { return stmt_type_; }

  /**
   * @returns Output expressions of the query
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetOutputExprs() const {
    return output_exprs_;
  }

  /**
   * @returns Physical properties of the output owned by QueryInfo
   */
  PropertySet *GetPhysicalProperties() const { return physical_props_; }

 private:
  /**
   * Type of the SQL Statement being executed
   */
  parser::StatementType stmt_type_;

  /**
   * Output Expressions of the query
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_exprs_;

  /**
   * Required physical properties of the output
   */
  PropertySet *physical_props_;
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
   * @param txn TransactionContext
   * @param accessor CatalogAccessor for catalog
   * @param storage StatsStorage
   * @param query_info Information about the query
   * @param op_tree Logical operator tree for execution
   * @returns execution plan
   */
  virtual std::unique_ptr<planner::AbstractPlanNode> BuildPlanTree(transaction::TransactionContext *txn,
                                                                   catalog::CatalogAccessor *accessor,
                                                                   StatsStorage *storage, QueryInfo query_info,
                                                                   std::unique_ptr<AbstractOptimizerNode> op_tree) = 0;

  /**
   * Reset the optimizer's internal state
   */
  virtual void Reset() {}
};

}  // namespace noisepage::optimizer
