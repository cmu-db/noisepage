#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Base class for table joins
 */
class AbstractJoinPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Base builder class for join plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    /**
     * @param predicate join predicate
     * @return builder object
     */
    ConcreteType &SetJoinPredicate(std::unique_ptr<const parser::AbstractExpression> predicate) {
      join_predicate_ = std::move(predicate);
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param type logical join type to use for join
     * @return builder object
     */
    ConcreteType &SetJoinType(LogicalJoinType type) {
      join_type_ = type;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * Logical join type
     */
    LogicalJoinType join_type_;
    /**
     * Join predicate
     */
    std::unique_ptr<const parser::AbstractExpression> join_predicate_;
  };

  /**
   * Base constructor for joins. Derived join plans should call this constructor
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  AbstractJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                       std::unique_ptr<const parser::AbstractExpression> predicate)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        join_type_(join_type),
        join_predicate_(std::move(predicate)) {}

 public:
  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

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
  const std::unique_ptr<const parser::AbstractExpression> &GetJoinPredicate() const { return join_predicate_; }

 private:
  LogicalJoinType join_type_;
  std::unique_ptr<const parser::AbstractExpression> join_predicate_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AbstractJoinPlanNode);
};

}  // namespace terrier::plan_node
