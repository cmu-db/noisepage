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

#include <memory>
#include <string>
#include <vector>
#include <common/macros.h>
#include <sstream>

#include "abstract_plannode.h"
#include "common/typedefs.h"
#include "common/hash_util.h"

namespace terrier::sql::plannode {

/**
 * Limit (with Offset) plan node
 */
class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(size_t limit, size_t offset) : limit_(limit), offset_(offset) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(LimitPlanNode);

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  const std::string GetInfo() const {
    std::stringstream ss;
    ss << "Limit[offset:" << offset_
       << ", limit: " << limit_ << "]";
    return ss.str();
  }

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

  bool LimitPlan::operator==(const AbstractPlanNode &rhs) const override {
    if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
      return false;
    }
    auto &other = static_cast<const LimitPlanNode &>(rhs);
    return (limit_ == other.limit_ && offset_ == other.offset_ && AbstractPlanNode::operator==(rhs));
  }
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Member Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  size_t GetLimit() const { return limit_; }

  size_t GetOffset() const { return offset_; }

 private:
  const size_t limit_;
  const size_t offset_;
};

}  // namespace terrier::sql::plannode
