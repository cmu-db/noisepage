//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// append_plan.h
//
// Identification: src/include/planner/append_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sql/plannode/abstract_plannode.h"

namespace terrier::sql::plannode {

/**
 * @brief Plan node for append.
 */
class AppendPlanNode : public AbstractPlanNode {
 public:
  AppendPlanNode() {}

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::APPEND; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new AppendPlanNode());
  }

 private:
  DISALLOW_COPY_AND_MOVE(AppendPlanNode);
};

}  // namespace terrier::sql::plannode
