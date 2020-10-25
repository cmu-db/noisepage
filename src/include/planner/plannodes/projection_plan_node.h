#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for projection
 */
class ProjectionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for projection plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the projection plan node
     * @return plan node
     */
    std::unique_ptr<ProjectionPlanNode> Build() {
      return std::unique_ptr<ProjectionPlanNode>(
          new ProjectionPlanNode(std::move(children_), std::move(output_schema_)));
    }
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  explicit ProjectionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                              std::unique_ptr<OutputSchema> output_schema)
      : AbstractPlanNode(std::move(children), std::move(output_schema)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  ProjectionPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(ProjectionPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::PROJECTION; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;
};

DEFINE_JSON_HEADER_DECLARATIONS(ProjectionPlanNode);

}  // namespace noisepage::planner
