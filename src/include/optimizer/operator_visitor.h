#pragma once

#include "optimizer/operators.h"

namespace terrier::optimizer {

/**
 * Utility class for visitor pattern
 */
class OperatorVisitor {
 public:
  virtual ~OperatorVisitor() = default;

  /**
   * Visit a TableFreeScan operator
   * @param table_free_scan operator
   */
  virtual void Visit(const TableFreeScan *table_free_scan) {}

  /**
   * Visit a SeqScan operator
   * @param seq_scan operator
   */
  virtual void Visit(const SeqScan *seq_scan) {}

  /**
   * Visit a IndexScan operator
   * @param index_scan operator
   */
  virtual void Visit(const IndexScan *index_scan) {}

  /**
   * Visit a ExternalFileScan operator
   * @param ext_file_scan operator
   */
  virtual void Visit(const ExternalFileScan *ext_file_scan) {}

  /**
   * Visit a QueryDerivedScan operator
   * @param query_derived_scan operator
   */
  virtual void Visit(const QueryDerivedScan *query_derived_scan) {}

  /**
   * Visit a OrderBy operator
   * @param order_by operator
   */
  virtual void Visit(const OrderBy *order_by) {}

  /**
   * Visit a Limit operator
   * @param limit operator
   */
  virtual void Visit(const Limit *limit) {}

  /**
   * Visit a InnerNLJoin operator
   * @param inner_join operator
   */
  virtual void Visit(const InnerNLJoin *inner_join) {}

  /**
   * Visit a LeftNLJoin operator
   * @param left_nl_join operator
   */
  virtual void Visit(const LeftNLJoin *left_nl_join) {}

  /**
   * Visit a RightNLJoin operator
   * @param right_nl_join operator
   */
  virtual void Visit(const RightNLJoin *right_nl_join) {}

  /**
   * Visit a OuterNLJoin operator
   * @param outer_nl_join operator
   */
  virtual void Visit(const OuterNLJoin *outer_nl_join) {}

  /**
   * Visit a InnerHashJoin operator
   * @param inner_hash_join operator
   */
  virtual void Visit(const InnerHashJoin *inner_hash_join) {}

  /**
   * Visit a LeftHashJoin operator
   * @param left_hash_join operator
   */
  virtual void Visit(const LeftHashJoin *left_hash_join) {}

  /**
   * Visit a RightHashJoin operator
   * @param right_hash_join operator
   */
  virtual void Visit(const RightHashJoin *right_hash_join) {}

  /**
   * Visit a OuterHashJoin operator
   * @param outer_hash_join operator
   */
  virtual void Visit(const OuterHashJoin *outer_hash_join) {}

  /**
   * Visit a Insert operator
   * @param insert operator
   */
  virtual void Visit(const Insert *insert) {}

  /**
   * Visit a InsertSelect operator
   * @param insert_select operator
   */
  virtual void Visit(const InsertSelect *insert_select) {}

  /**
   * Visit a Delete operator
   * @param del operator
   */
  virtual void Visit(const Delete *del) {}

  /**
   * Visit a Update operator
   * @param update operator
   */
  virtual void Visit(const Update *update) {}

  /**
   * Visit a HashGroupBy operator
   * @param hash_group_by operator
   */
  virtual void Visit(const HashGroupBy *hash_group_by) {}

  /**
   * Visit a SortGroupBy operator
   * @param sort_group_by operator
   */
  virtual void Visit(const SortGroupBy *sort_group_by) {}

  /**
   * Visit a Distinct operator
   * @param distinct operator
   */
  virtual void Visit(const Distinct *distinct) {}

  /**
   * Visit a Aggregate operator
   * @param aggregate operator
   */
  virtual void Visit(const Aggregate *aggregate) {}

  /**
   * Visit a ExportExternalFile operator
   * @param export_ext_file operator
   */
  virtual void Visit(const ExportExternalFile *export_ext_file) {}

  /**
   * Visit a LogicalGet operator
   * @param logical_get operator
   */
  virtual void Visit(const LogicalGet *logical_get) {}

  /**
   * Visit a LogicalExternalFileGet operator
   * @param logical_external_file_get operator
   */
  virtual void Visit(const LogicalExternalFileGet *logical_external_file_get) {}

  /**
   * Visit a LogicalQueryDerivedGet operator
   * @param logical_query_derived_get operator
   */
  virtual void Visit(const LogicalQueryDerivedGet *logical_query_derived_get) {}

  /**
   * Visit a LogicalFilter operator
   * @param logical_filter operator
   */
  virtual void Visit(const LogicalFilter *logical_filter) {}

  /**
   * Visit a LogicalProjection operator
   * @param logical_projection operator
   */
  virtual void Visit(const LogicalProjection *logical_projection) {}

  /**
   * Visit a LogicalMardJoin operator
   * @param logical_mark_join operator
   */
  virtual void Visit(const LogicalMarkJoin *logical_mark_join) {}

  /**
   * Visit a LogicalSingleJoin operator
   * @param logical_single_join operator
   */
  virtual void Visit(const LogicalSingleJoin *logical_single_join) {}

  /**
   * Visit a LogicalDependentJoin operator
   * @param logical_dependent_join operator
   */
  virtual void Visit(const LogicalDependentJoin *logical_dependent_join) {}

  /**
   * Vusut a LogicalInnerJoin operator
   * @param logical_inner_join operator
   */
  virtual void Visit(const LogicalInnerJoin *logical_inner_join) {}

  /**
   * Visit a LogicalLeftJoin operator
   * @param logical_left_join operator
   */
  virtual void Visit(const LogicalLeftJoin *logical_left_join) {}

  /**
   * Visit a LogicalRightJoin operator
   * @param logical_right_join operator
   */
  virtual void Visit(const LogicalRightJoin *logical_right_join) {}

  /**
   * Visit a LogicalOuterJoin operator
   * @param logical_outer_join operator
   */
  virtual void Visit(const LogicalOuterJoin *logical_outer_join) {}

  /**
   * Visit a LogicalSemiJoin operator
   * @param logical_semi_join operator
   */
  virtual void Visit(const LogicalSemiJoin *logical_semi_join) {}

  /**
   * Visit a LogicalAggregateAndGroupBy operator
   * @param logical_aggregate_and_group_by operator
   */
  virtual void Visit(const LogicalAggregateAndGroupBy *logical_aggregate_and_group_by) {}

  /**
   * Visit a LogicalInsert operator
   * @param logical_insert operator
   */
  virtual void Visit(const LogicalInsert *logical_insert) {}

  /**
   * Visit a LogicalInsertSelect operator
   * @param logical_insert_select operator
   */
  virtual void Visit(const LogicalInsertSelect *logical_insert_select) {}

  /**
   * Visit a LogicalDelete operator
   * @param logical_delete operator
   */
  virtual void Visit(const LogicalDelete *logical_delete) {}

  /**
   * Visit a LogicalUpdata operator
   * @param logical_update operator
   */
  virtual void Visit(const LogicalUpdate *logical_update) {}

  /**
   * Visit a LogicalDistinct operator
   * @param logical_distinct operator
   */
  virtual void Visit(const LogicalDistinct *logical_distinct) {}

  /**
   * Visit a LogicalLimit operator
   * @param logical_limit operator
   */
  virtual void Visit(const LogicalLimit *logical_limit) {}

  /**
   * Visit a LogicalExportExternalFile operator
   * @param logical_export_external_file operator
   */
  virtual void Visit(const LogicalExportExternalFile *logical_export_external_file) {}
};

}  // namespace terrier::optimizer
