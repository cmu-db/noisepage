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

namespace terrier::sql::plannode {

/**
 * Base class for all scan plan nodes
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  // We should add an empty constructor to support an empty object
  AbstractScanPlanNode() : target_table_(nullptr), predicate_(nullptr), parallel_(false) {}

  AbstractScanPlanNode(storage::DataTable *table, expression::AbstractExpression *predicate,
                       const std::vector<oid_t> &column_ids, bool parallel)
      : target_table_(table), predicate_(predicate), column_ids_(column_ids), parallel_(parallel) {}

  // const expression::AbstractExpression *GetPredicate() const { return predicate_.get(); }

  const std::vector<col_oid_t> &GetColumnIds() const { return column_ids_; }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns.resize(GetColumnIds().size());
    std::iota(columns.begin(), columns.end(), 0);
  }

  virtual void GetAttributes(std::vector<const AttributeInfo *> &ais) const {
    for (const auto &ai : attributes_) {
      ais.push_back(&ai);
    }
  }

  bool IsForUpdate() const { return is_for_update_; }

  bool IsParallel() const { return parallel_; }

 protected:
  void AddColumnId(oid_t col_id) { column_ids_.push_back(col_id); }

  void SetPredicate(expression::AbstractExpression *predicate) {
    predicate_ = std::unique_ptr<expression::AbstractExpression>(predicate);
  }

  void SetForUpdateFlag(bool flag) { is_for_update_ = flag; }

  const std::string GetPredicateInfo() const { return predicate_ != nullptr ? predicate_->GetInfo() : ""; }

 private:
  // Target table for this scan
  table_oid_t target_table;

  // Selection predicate. We remove const to make it used when deserialization
  std::unique_ptr<expression::AbstractExpression> predicate_;

  // Columns to be added to output
  std::vector<col_oid_t> column_ids_;
  std::vector<AttributeInfo> attributes_;

  // Are the tuples produced by this plan intended for update?
  bool is_for_update_ = false;

  // Should this scan be performed in parallel?
  bool parallel_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::sql::plannode
