#pragma once

#include "abstract_plan_node.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::plan_node {

//===--------------------------------------------------------------------===//
// Abstract Join Plan Node
//===--------------------------------------------------------------------===//

class AbstractJoinPlanNode : public AbstractPlanNode {
 public:
  AbstractJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                       parser::AbstractExpression *predicate)
      : AbstractPlanNode(output_schema), join_type_(join_type), predicate_(predicate) {}

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  LogicalJoinType GetLogicalJoinType() const { return join_type_; }

  const parser::AbstractExpression *GetPredicate() const { return predicate_; }

 private:
  LogicalJoinType join_type_;

  const parser::AbstractExpression *predicate_;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractJoinPlanNode);
};

}  // namespace terrier::plan_node