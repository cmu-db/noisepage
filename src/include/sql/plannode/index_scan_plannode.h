//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.h
//
// Identification: src/include/planner/index_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "sql/plannode/plannode_defs.h"
#include "sql/plannode/abstract_scan_plannode.h"

namespace terrier::sql::plannode {

class IndexScanPlanNode : public AbstractScanPlanNode {
 public:

  IndexScanPlanNode(table_oid_t table,
                expression::AbstractExpression *predicate,
                const std::vector<col_oid_t> &column_ids,
                bool is_parallel)
      : AbstractScanPlanNode(table, predicate, column_ids, is_parallel) {}

  ~IndexScanPlanNode() {
    for (auto expr : runtime_keys_) {
      delete expr;
    }
  }

  ///////////////////////////////////////////////////////////////////
  // Interface API
  ///////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::INDEXSCAN;
  }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    std::vector<expression::AbstractExpression *> new_runtime_keys;
    for (auto *key : runtime_keys_) {
      new_runtime_keys.push_back(key->Copy());
    }

    IndexScanPlanNode *new_plan = new IndexScanPlanNode(
        GetTableId(), GetPredicate()->Copy(), GetOutputColumnIds(), IsParallel());

    // FIXME: Need to copy the other internal members

    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  ///////////////////////////////////////////////////////////////////
  // PlanNode Specific API
  ///////////////////////////////////////////////////////////////////

  /**
   * Get the target index for this scan operator
   * @return
   */
  index_oid_t GetIndexId() const { return index_id_; }

  /**
   * A list of columns id in the base table that has a scan predicate
   * (i.e., only for indexed column in the base table)
   * @return
   */
  const std::vector<col_oid_t> &GetTupleColumnIds() const {
    return tuple_column_ids_;
  }

  /**
   *
   * @return
   */
  const std::vector<col_oid_t> &GetKeyColumnIds() const {
    return key_column_ids_;
  }

  /**
   *
   * @return
   */
  const std::vector<ExpressionType> &GetExprTypes() const {
    return expr_types_;
  }

  /**
   *
   * @return
   */
  const std::vector<expression::AbstractExpression *> &GetRunTimeKeys() const {
    return runtime_keys_;
  }

  /**
   *
   * @return
   */
  bool GetLeftOpen() const { return left_open_; }

  /**
   *
   * @return
   */
  bool GetRightOpen() const { return right_open_; }

  /**
   *
   * @return
   */
  bool GetDescend() const { return descend_; }

  /**
   *
   * @param descend
   */
  void SetDescend(bool descend) { descend_ = descend; }

 private:
  /**
   * index associated with index scan.
   */
  index_oid_t index_id_;

  /**
   * A list of column IDs involved in the scan no matter whether
   * it is indexed or not (i.e. select statement)
   */
  const std::vector<col_oid_t> tuple_column_ids_;

  /**
   * A list of column IDs involved in the index scan that are indexed by
   * the index selected inside the optimizer
   */
  const std::vector<col_oid_t> key_column_ids_;

  /**
   * The list of expressions on the index
   */
  const std::vector<ExpressionType> expr_types_;

  /**
   *
   */
  std::vector<expression::AbstractExpression *> runtime_key_list_;

  /**
   * LM: I removed a const keyword for binding purpose
   * The life time of the scan predicate is as long as the lifetime
   * of this array, since we always use the values in this array to
   * construct low key and high key for the scan plan, it should stay
   * valid until the scan plan is destructed
   *
   * Note that when binding values to the scan plan we copy those values
   * into this array, which means the lifetime of values being bound is
   * also the lifetime of the IndexScanPlanNode object
   */
  std::vector<type::Value> values_;

  /**
   * The original copy of values with all the value parameters (bind them later)
   */
  std::vector<type::Value> values_with_params_;

  /**
   * TODO: What the hell is this for???
   */
  const std::vector<expression::AbstractExpression *> runtime_keys_;

  /**
   * whether the index scan range is left open
   */
  bool left_open_ = false;

  /**
   * whether the index scan range is right open
   */
  bool right_open_ = false;

  /**
   * whether order by is descending
   */
  bool descend_ = false;

 private:
  DISALLOW_COPY_AND_MOVE(IndexScanPlanNode);
};

}  // namespace planner
}  // namespace peloton
