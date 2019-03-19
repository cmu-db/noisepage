#pragma once

#include <memory>
#include <utility>
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Base class for table joins
 */
class AbstractJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * Base constructor for joins. Derived join plans should call this constructor
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  AbstractJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                       parser::AbstractExpression *predicate)
      : AbstractPlanNode(std::move(output_schema)), join_type_(join_type), predicate_(predicate) {}

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  /**
   * @return logical join type
   */
  LogicalJoinType GetLogicalJoinType() const { return join_type_; }

  /**
   * @return pointer to predicate used for join
   */
  const parser::AbstractExpression *GetPredicate() const { return predicate_; }

 private:
  LogicalJoinType join_type_;

  const parser::AbstractExpression *predicate_;

 public:
  DISALLOW_COPY_AND_MOVE(AbstractJoinPlanNode);
};

}  // namespace terrier::plan_node
