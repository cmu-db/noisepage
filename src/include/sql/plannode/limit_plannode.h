//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_plan.h
//
// Identification: src/include/planner/limit_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <common/macros.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/hash_util.h"
#include "common/typedefs.h"
#include "sql/plannode/abstract_plannode.h"

namespace terrier::sql::plannode {

/**
 * Limit (with Offset) plan node
 */
class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(size_t limit, size_t offset) : limit_(limit), offset_(offset) {}

  ///////////////////////////////////////////////////////////////////
  // Interface API
  ///////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  void GetOutputColumns(std::vector<col_oid_t> &columns) const override {
    TERRIER_ASSERT(GetChildrenSize() == 1, "LimitPlanNode expected to have one child");
    GetChild(0)->GetOutputColumns(columns);
  }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new LimitPlanNode(limit_, offset_));
  }

  common::hash_t Hash() const override {
    auto type = GetPlanNodeType();
    common::hash_t hash = common::HashUtil::Hash(&type);
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));
    return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
  }

  bool operator==(const AbstractPlanNode &rhs) const override {
    if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
      return false;
    }
    auto &other = static_cast<const LimitPlanNode &>(rhs);
    return (limit_ == other.limit_ && offset_ == other.offset_ && AbstractPlanNode::operator==(rhs));
  }

  ///////////////////////////////////////////////////////////////////
  // PlanNode Specific API
  ///////////////////////////////////////////////////////////////////

  size_t GetLimit() const { return limit_; }

  size_t GetOffset() const { return offset_; }

 private:
  const size_t limit_;
  const size_t offset_;

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(LimitPlanNode);
};

}  // namespace terrier::sql::plannode
