//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.h
//
// Identification: src/include/planner/hash_join_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numeric>

#include "abstract_join_plannode.h"
#include "plannode_defs.h"
#include "common/hash_util.h"

namespace terrier::sql::plannode {

class HashJoinPlanNode : public AbstractJoinPlanNode {
  using ExpressionPtr = std::unique_ptr<const expression::AbstractExpression>;

 public:
  HashJoinPlanNode(JoinType join_type, ExpressionPtr &&predicate,
               std::unique_ptr<const ProjectInfo> &&proj_info,
               std::shared_ptr<const catalog::Schema> &proj_schema,
               std::vector<ExpressionPtr> &left_hash_keys,
               std::vector<ExpressionPtr> &right_hash_keys,
               bool build_bloomfilter = false) : AbstractJoinPlanNode(join_type,
                                                                      std::move(predicate),
                                                                      std::move(proj_info),
                                                                      proj_schema),
                                                 left_hash_keys_(std::move(left_hash_keys)),
                                                 right_hash_keys_(std::move(right_hash_keys)),
                                                 build_bloomfilter_(build_bloomfilter) {}


  void HandleSubplanBinding(bool is_left, const BindingContext &input) override;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::HASHJOIN;
  }

  bool IsBloomFilterEnabled() const { return build_bloomfilter_; }

  void SetBloomFilterFlag(bool flag) { build_bloomfilter_ = flag; }

  const std::string GetInfo() const override { return "HashJoinPlan"; }

  void GetLeftHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const;

  void GetRightHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utils
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::unique_ptr<AbstractPlanNode> Copy() const override;

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  DISALLOW_COPY_AND_MOVE(HashJoinPlanNode);

  // The left and right expressions that constitute the join keys
  std::vector<ExpressionPtr> left_hash_keys_;
  std::vector<ExpressionPtr> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;
};

}  // namespace terrier:sql::plannode
