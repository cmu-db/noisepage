#pragma once

#include "abstract_plan_node.h"
#include "abstract_scan_plan_node.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::plan_node {

class SeqScanPlanNode : public AbstractScanPlanNode {
 public:
  SeqScanPlanNode(catalog::Schema output_schema, parser::AbstractExpression *predicate, bool is_for_update = false,
                  bool parallel = false)
      : AbstractScanPlanNode(output_schema, predicate, is_for_update, parallel) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SEQSCAN; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    auto *new_plan = new SeqScanPlanNode(GetOutputSchema(), GetPredicate()->Copy().get(), IsForUpdate(), IsParallel());
    return std::unique_ptr<AbstractPlanNode>(new_plan);
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  DISALLOW_COPY_AND_MOVE(SeqScanPlanNode);
};

}  // namespace terrier::plan_node
