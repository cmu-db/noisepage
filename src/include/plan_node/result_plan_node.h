#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"
#include "storage/storage_defs.h"

// TODO(Gus,Wen) Tuple as a concept does not exist yet, someone need to define it in the storage layer, possibly a
// collection of TransientValues
namespace terrier::plan_node {

/**
 * Result plan node.
 * The counter-part of Postgres Result plan
 * that returns a single constant tuple.
 */
class ResultPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for an delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param tuple the tuple in the storage layer
     * @return builder object
     */
    Builder &SetTuple(std::shared_ptr<Tuple> tuple) {
      tuple_ = std::move(tuple);
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::shared_ptr<ResultPlanNode> Build() {
      return std::shared_ptr<ResultPlanNode>(new ResultPlanNode(std::move(children_), std::move(output_schema_),
                                                                estimated_cardinality_, std::move(tuple_)));
    }

   protected:
    std::shared_ptr<Tuple> tuple_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param tuple the tuple in the storage layer
   */
  ResultPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 uint32_t estimated_cardinality, std::shared_ptr<Tuple> tuple)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        tuple_(std::move(tuple)) {}

 public:
  /**
   * @return the tuple in the storage layer
   */
  const std::shared_ptr<Tuple> GetTuple() const { return tuple_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::RESULT; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  // the tuple in the storage layer
  std::shared_ptr<Tuple> tuple_;

 public:
  DISALLOW_COPY_AND_MOVE(ResultPlanNode);
};

}  // namespace terrier::plan_node
