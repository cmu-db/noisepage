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
  SeqScanPlanNode(table_oid_t table, expression::AbstractExpression *predicate,
                  const std::vector<col_oid_t> &column_ids, bool is_parallel)
      : AbstractScanPlanNode(table, predicate, column_ids, is_parallel) {}

  ///////////////////////////////////////////////////////////////////
  // Interface API
  ///////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    auto *new_plan = new SeqScanPlanNode(GetTableId(), GetPredicate()->Copy(), GetOutputColumnIds(), IsParallel());
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode);
};

}  // namespace terrier::sql::plannode
