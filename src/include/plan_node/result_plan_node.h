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
 public:
  /**
   * Instantiate a Result Plan Node
   * @param outputSchema the schema of the output node
   * @param tuple the tuple in the storage layer
   */
  ResultPlanNode(std::shared_ptr<OutputSchema> outputSchema, std::shared_ptr<Tuple> tuple)
      : AbstractPlanNode(std::move(outputSchema)), tuple_(std::move(tuple)) {}

  /**
   * @return the tuple in the storage layer
   */
  const std::shared_ptr<Tuple> GetTuple() const { return tuple_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::RESULT; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new ResultPlanNode(GetOutputSchema(), tuple_));
  }

 private:
  // the tuple in the storage layer
  std::shared_ptr<Tuple> tuple_;

 private:
  DISALLOW_COPY_AND_MOVE(ResultPlanNode);
};

}  // namespace terrier::plan_node
