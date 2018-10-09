//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.h
//
// Identification: src/include/planner/seq_scan_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "sql/plannode/abstract_scan_plannode.h"

namespace terrier::sql::plannode {

class SeqScanPlanNode : public AbstractScanPlanNode {
 public:
  SeqScanPlanNode(table_oid_t target_table, expression::AbstractExpression *predicate,
                  const std::vector<col_oid_t> &column_ids, bool parallel, bool is_for_update)
      : AbstractScanPlanNode(table, predicate, column_ids, parallel, is_for_update) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  std::unique_ptr<AbstractPlan> Copy() const override {
    auto *new_plan =
        new SeqScanPlanNode(GetTable(), GetPredicate()->Copy(), GetColumnIds(), IsParallel(), IsForUpdate());
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override { return !(*this == rhs); }

 private:
  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode);
};

}  // namespace terrier::sql::plannode
