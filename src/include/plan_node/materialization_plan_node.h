#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"

namespace terrier::plan_node {

class MaterializationPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for a materialization plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param materialize_flag Whether to create a physical tile or just pass through underlying logical tile
     * @return builder object
     */
    Builder &SetMaterializeFlag(bool materialize_flag) {
      materialize_flag_ = materialize_flag;
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::shared_ptr<MaterializationPlanNode> Build() {
      return std::shared_ptr<MaterializationPlanNode>(new MaterializationPlanNode(
          std::move(children_), std::move(output_schema_), estimated_cardinality_, materialize_flag_));
    }

   protected:
    bool materialize_flag_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param set_op the set pperation of this node
   */
  MaterializationPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                          std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                          bool materialize_flag)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        materialize_flag_(materialize_flag) {}

 public:
  /**
   * @return materialization flag
   */
  bool GetMaterializeFlag() const { return materialize_flag_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::MATERIALIZE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * Whether to create a physical tile or just pass through underlying
   * logical tile
   */
  bool materialize_flag_;

 public:
  DISALLOW_COPY_AND_MOVE(MaterializationPlanNode);
};

}  // namespace terrier::plan_node
