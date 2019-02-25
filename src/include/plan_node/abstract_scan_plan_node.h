#pragma once

#include "abstract_plan_node.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::plan_node {

class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  AbstractScanPlanNode(catalog::Schema output_schema, parser::AbstractExpression *predicate, bool is_for_update,
                       bool parallel)
      : AbstractPlanNode(output_schema), predicate_(predicate), parallel_(parallel), is_for_update_(is_for_update) {}

  const parser::AbstractExpression *GetPredicate() const { return predicate_.get(); }

  bool IsForUpdate() const { return is_for_update_; }

  bool IsParallel() const { return parallel_; }

 private:
  // Selection predicate. We remove const to make it used when deserialization
  std::unique_ptr<parser::AbstractExpression> predicate_;

  // Should this scan be performed in parallel?
  bool parallel_;

  // Are the tuples produced by this plan intended for update?
  bool is_for_update_ = false;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::plan_node
