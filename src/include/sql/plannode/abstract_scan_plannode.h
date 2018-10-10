//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_scan_plan.h
//
// Identification: src/include/planner/abstract_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/typedefs.h"
#include "sql/plannode/abstract_plannode.h"

namespace terrier::sql::expression {
class AbstractExpression;
class AttributeInfo;
}  // namespace terrier::sql::expression

namespace terrier::sql::plannode {

/**
 * Base class for all scan plan nodes
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  /**
   *
   * @param target_table
   * @param predicate
   * @param column_ids
   * @param parallel
   */
  AbstractScanPlanNode(table_oid_t target_table, expression::AbstractExpression *predicate,
                       const std::vector<col_oid_t> &column_ids, bool parallel, bool is_for_update)
      : target_table_(target_table),
        predicate_(predicate),
        column_ids_(column_ids),
        parallel_(parallel),
        is_for_update_(is_for_update) {}

  /**
   *
   * @return
   */
  const expression::AbstractExpression *GetPredicate() const { return predicate_.get(); }

  /**
   * Retrieve the list of output column oids
   * @return
   */
  const std::vector<col_oid_t> &GetColumnIds() const { return column_ids_; }

  /**
   * Retrieve the list of AttributeInfo objectds for this node's output columns
   * @param ais
   */
  virtual const std::vector<const expression::AttributeInfo> GetAttributes() const { return attributes_; }

  /**
   * Returns true if this scan is marking tuples that it reads for updating
   * in the transaction.
   * @return
   */
  bool IsForUpdate() const { return is_for_update_; }

  /**
   * Returns true if this is a parallel scan operator
   * @return
   */
  bool IsParallel() const { return parallel_; }

 protected:
  /**
   *
   * @param col_id
   */
  void AddColumnId(col_oid_t col_id) { column_ids_.push_back(col_id); }

  /**
   *
   * @param predicate
   */
  void SetPredicate(expression::AbstractExpression *predicate) {
    predicate_ = std::unique_ptr<expression::AbstractExpression>(predicate);
  }

 private:
  // Target table for this scan
  table_oid_t target_table_;

  // Selection predicate. We remove const to make it used when deserialization
  std::unique_ptr<expression::AbstractExpression> predicate_;

  // Columns to be added to output
  std::vector<col_oid_t> column_ids_;
  std::vector<expression::AttributeInfo> attributes_;

  // Are the tuples produced by this plan intended for update?
  bool is_for_update_ = false;

  // Should this scan be performed in parallel?
  bool parallel_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::sql::plannode
