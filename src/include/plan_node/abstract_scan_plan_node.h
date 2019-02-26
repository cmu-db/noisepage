#pragma once

#include "abstract_plan_node.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::plan_node {

class AbstractScanPlanNode : public AbstractPlanNode {
 public:
  AbstractScanPlanNode(catalog::Schema output_schema, parser::AbstractExpression *predicate, bool is_for_update = false,
                       bool parallel = false)
      : AbstractPlanNode(output_schema), predicate_(predicate), is_for_update_(is_for_update), parallel_(parallel) {}

  const parser::AbstractExpression *GetPredicate() const { return predicate_.get(); }

  bool IsForUpdate() const { return is_for_update_; }

  bool IsParallel() const { return parallel_; }

 private:
  // Selection predicate. We remove const to make it used when deserialization
  std::unique_ptr<parser::AbstractExpression> predicate_;

  // Are the tuples produced by this plan intended for update?
  bool is_for_update_ = false;

  // Should this scan be performed in parallel?
  bool parallel_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode);
};

}  // namespace terrier::plan_node
