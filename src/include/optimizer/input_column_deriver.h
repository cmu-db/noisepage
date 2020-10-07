#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/managed_pointer.h"
#include "optimizer/operator_visitor.h"
#include "transaction/transaction_context.h"

namespace planner {
enum class AggregateStrategyType;
}

namespace terrier::optimizer {

class PropertySet;
class GroupExpression;
class OperatorNode;
class Memo;
class BaseOperatorNodeContents;

/**
 * InputColumnDeriver generate input and output columns based on the required columns,
 * required properties and the current group expression. We use the input/output
 * columns to eventually generate plans
 */
class InputColumnDeriver : public OperatorVisitor {
 public:
  /**
   * Constructor
   * @param txn TransactionContext
   * @param accessor CatalogAccessor
   */
  InputColumnDeriver(transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor)
      : accessor_(accessor), txn_(txn) {}

  /**
   * Derives the input and output columns for a physical operator
   * @param gexpr Group Expression to derive for
   * @param properties Relevant data properties
   * @param required_cols Vector of required output columns
   * @param memo Memo
   * @returns pair where first element is the output columns and the second
   *          element is a vector of inputs from each child.
   *
   * Pointers returned are not ManagedPointer. However, the returned pointers
   * should not be deleted or ever modified.
   */
  std::pair<std::vector<common::ManagedPointer<parser::AbstractExpression>>,
            std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>
  DeriveInputColumns(GroupExpression *gexpr, PropertySet *properties,
                     std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols, Memo *memo);

  /**
   * Visit function to derive input/output columns for TableFreeScan
   * @param op TableFreeScan operator to visit
   */
  void Visit(const TableFreeScan *op) override;

  /**
   * Visit function to derive input/output columns for SeqScan
   * @param op SeqScan operator to visit
   */
  void Visit(const SeqScan *op) override;

  /**
   * Visit function to derive input/output columns for IndexScan
   * @param op IndexScan operator to visit
   */
  void Visit(const IndexScan *op) override;

  /**
   * Visit function to derive input/output columns for ExternalFileScan
   * @param op ExternalFileScan operator to visit
   */
  void Visit(const ExternalFileScan *op) override;

  /**
   * Visit function to derive input/output columns for QueryDerivedScan
   * @param op QueryDerivedScan operator to visit
   */
  void Visit(const QueryDerivedScan *op) override;

  /**
   * Visit function to derive input/output columns for OrderBy
   * @param op OrderBy operator to visit
   */
  void Visit(const OrderBy *op) override;

  /**
   * Visit function to derive input/output columns for Limit
   * @param op Limit operator to visit
   */
  void Visit(const Limit *op) override;

  /**
   * Visit function to derive input/output columns for InnerIndexJoin
   * @param op InnerIndexJoin operator to visit
   */
  void Visit(const InnerIndexJoin *op) override;

  /**
   * Visit function to derive input/output columns for InnerNLJoin
   * @param op InnerNLJoin operator to visit
   */
  void Visit(const InnerNLJoin *op) override;

  /**
   * Visit function to derive input/output columns for LeftNLJoin
   * @param op LeftNLJoin operator to visit
   */
  void Visit(const LeftNLJoin *op) override;

  /**
   * Visit function to derive input/output columns for RightNLJoin
   * @param op RightNLJoin operator to visit
   */
  void Visit(const RightNLJoin *op) override;

  /**
   * Visit function to derive input/output columns for OuterNLJoin
   * @param op OuterNLJoin operator to visit
   */
  void Visit(const OuterNLJoin *op) override;

  /**
   * Visit function to derive input/output columns for InnerHashJoin
   * @param op InnerHashJoin operator to visit
   */
  void Visit(const InnerHashJoin *op) override;

  /**
   * Visit function to derive input/output columns for LeftHashJoin
   * @param op LeftHashJoin operator to visit
   */
  void Visit(const LeftHashJoin *op) override;

  /**
   * Visit function to derive input/output columns for RightHashJoin
   * @param op RightHashJoin operator to visit
   */
  void Visit(const RightHashJoin *op) override;

  /**
   * Visit function to derive input/output columns for OuterHashJoin
   * @param op OuterHashJoin operator to visit
   */
  void Visit(const OuterHashJoin *op) override;

  /**
   * Visit function to derive input/output columns for LeftSemiHashJoin
   * @param op LeftSemiHashJoin operator to visit
   */
  void Visit(const LeftSemiHashJoin *op) override;

  /**
   * Visit function to derive input/output columns for TableFreeScan
   * @param op TableFreeScan operator to visit
   */
  void Visit(const Insert *op) override;

  /**
   * Visit function to derive input/output columns for InsertSelect
   * @param op InsertSelectoperator to visit
   */
  void Visit(const InsertSelect *op) override;

  /**
   * Visit function to derive input/output columns for Delete
   * @param op Delete operator to visit
   */
  void Visit(const Delete *op) override;

  /**
   * Visit function to derive input/output columns for Update
   * @param op Update operator to visit
   */
  void Visit(const Update *op) override;

  /**
   * Visit function to derive input/output columns for HashGroupBy
   * @param op HashGroupBy operator to visit
   */
  void Visit(const HashGroupBy *op) override;

  /**
   * Visit function to derive input/output columns for SortGroupBy
   * @param op SortGroupBy operator to visit
   */
  void Visit(const SortGroupBy *op) override;

  /**
   * Visit function to derive input/output columns for Aggregate
   * @param op Aggregate operator to visit
   */
  void Visit(const Aggregate *op) override;

  /**
   * Visit function to derive input/output columns for ExportExternalFile
   * @param op ExportExternalFile operator to visit
   */
  void Visit(const ExportExternalFile *op) override;

 private:
  /**
   * Helper to derive the output columns of a scan operator.
   * A scan operator has no input columns, and the output columns are the set
   * of all columns required (i.e required_cols_).
   */
  void ScanHelper();

  /**
   * Derive all input and output columns for an Aggregate
   * The output columns are all Tuple and Aggregation columns from required_cols_
   * and any extra columns used by having expressions.
   *
   * The input column vector contains of a single vector consisting of all
   * TupleValueExpressions needed by GroupBy and Having expressions and any
   * TupleValueExpressions required by AggregateExpression from required_cols_.
   *
   * @param op Visiting BaseOperatorNode
   */
  void AggregateHelper(const BaseOperatorNodeContents *op);

  /**
   * Derives the output and input columns for a Join.
   *
   * The output columns are all the TupleValueExpression and AggregateExpressions
   * as located in required_cols_. The set of all input columns are built by consolidating
   * all TupleValueExpression and AggregateExpression in all left_keys/right_keys/join_conds
   * and any in required_cols_ are are split as input_cols = {build_cols, probe_cols}
   * based on build-side table aliases and probe-side table aliases.
   *
   * NOTE:
   * - This function assumes the build side is the Left Child
   * - This function assumes the probe side is the Right Child
   *
   * @param op Visiting BaseOperatorNode
   */
  void JoinHelper(const BaseOperatorNodeContents *op);

  /**
   * Passes down the list of required columns as input columns
   * Sets output_input_cols_ = (required_cols, {required_cols_})
   */
  void Passdown();

  /**
   * Generates input set to be all columns of the base table
   * @param alias Table Alias
   * @param db DB OID
   * @param tbl Table OID
   */
  void InputBaseTableColumns(const std::string &alias, catalog::db_oid_t db, catalog::table_oid_t tbl);

  /**
   * GroupExpression analyzing
   */
  GroupExpression *gexpr_;

  /**
   * Memo
   */
  Memo *memo_;

  /**
   * The derived output columns and input columns, note that the current
   * operator may have more than one children
   */
  std::pair<std::vector<common::ManagedPointer<parser::AbstractExpression>>,
            std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>
      output_input_cols_;

  /**
   * The required columns
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols_;

  /**
   * The required physical properties
   */
  PropertySet *properties_;

  /**
   * CatalogAccessor
   */
  catalog::CatalogAccessor *accessor_;

  /**
   * TransactionContext
   */
  transaction::TransactionContext *txn_;
};

}  // namespace terrier::optimizer
