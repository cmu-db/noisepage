#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_plan_node.h"
#include "output_schema.h"
#include "storage/storage_defs.h"

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
   * @param tuple_slot the slot of the tuple in the storage layer
   */
  ResultPlanNode(OutputSchema outputSchema, storage::TupleSlot tuple_slot)
      : AbstractPlanNode(outputSchema), tuple_slot_(tuple_slot) {}

  /**
   * @return the slot of the tuple in the storage layer
   */
  const shared_ptr<storage::TupleSlot> GetTupleSlot() const { return tuple_slot_; }

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::RESULT; }

  /**
   * @return debug info
   */
  inline std::string GetInfo() const { return "Result Plan Node"; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(
        new ResultPlanNode(new storage::TupleSlot(tuple_slot_));
  }

 private:
  // the slot of the tuple in the storage layer
  std::unique_ptr<storage::TupleSlot> tuple_slot_;

 private:
  DISALLOW_COPY_AND_MOVE(ResultPlanNode);
};

}  // namespace terrier::plan_node