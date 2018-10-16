//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.h
//
// Identification: src/include/planner/delete_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <common/macros.h>
#include "abstract_plannode.h"
#include "common/typedefs.h"

namespace terrier::sql::plannode {

class DeletePlanNode : public AbstractPlanNode {
 public:
  DeletePlanNode() = delete;

  ~DeletePlanNode() {}

  DeletePlanNode(storage::DataTable *table) : target_table_(table) {}

  storage::DataTable *GetTable() const { return target_table_; }

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  const std::string GetInfo() const override { return "DeletePlan"; }

  // TODO: Possibly delete this
  void SetParameterValues(std::vector<type::Value> *values) override;

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new DeletePlan(target_table_));
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override {
    return !(*this == rhs);
  }

 private:
  storage::DataTable *target_table_ = nullptr;

 private:
  DISALLOW_COPY_AND_MOVE(DeletePlan);
};

}  // namespace terrier::sql::plannode

