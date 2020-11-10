#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace noisepage::planner {

/**
 * Base class for table joins.
 */
class AbstractJoinPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Base builder class for join plan nodes.
   * @tparam ConcreteType The concrete type of plan node this builder is building.
   */
  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    /**
     * @param predicate join predicate
     * @return builder object
     */
    ConcreteType &SetJoinPredicate(common::ManagedPointer<parser::AbstractExpression> predicate) {
      join_predicate_ = predicate;
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
    common::ManagedPointer<parser::AbstractExpression> join_predicate_;
  };

  /**
   * Base constructor for joins. Derived join plans should call this constructor.
   * @param children All children to the join.
   * @param output_schema Schema representing the structure of the output of this plan node.
   * @param join_type The logical join type.
   * @param predicate The join predicate.
   */
  AbstractJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                       common::ManagedPointer<parser::AbstractExpression> predicate);

 public:
  /**
   * Default constructor used for deserialization
   */
  AbstractJoinPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AbstractJoinPlanNode)

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  /**
   * @return The logical join type.
   */
  LogicalJoinType GetLogicalJoinType() const { return join_type_; }

  /**
   * @return True if this join requires a left-mark. Semi, Anti and Left-outer joins requires marks
   *         on the left side to indicate if the left-tuple found a join partner.
   */
  bool RequiresLeftMark() const {
    switch (join_type_) {
      case LogicalJoinType::LEFT:
      case LogicalJoinType::SEMI:
      case LogicalJoinType::ANTI:
      case LogicalJoinType::OUTER:
      case LogicalJoinType::LEFT_SEMI:
        return true;
      default:
        return false;
    }
  }

  /**
   * @return True if the join requires a right-mark.
   */
  bool RequiresRightMark() const {
    switch (join_type_) {
      case LogicalJoinType::RIGHT_SEMI:
      case LogicalJoinType::RIGHT_ANTI:
        return true;
      default:
        return false;
    }
  }

  /**
   * @return The predicate used for join.
   */
  common::ManagedPointer<parser::AbstractExpression> GetJoinPredicate() const { return join_predicate_; }

 private:
  LogicalJoinType join_type_;
  common::ManagedPointer<parser::AbstractExpression> join_predicate_;
};

}  // namespace noisepage::planner
