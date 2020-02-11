#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "common/managed_pointer.h"
#include "planner/plannodes/plan_visitor.h"
#include "execution/compiler/operator/operator_translator.h"
#include "brain/operating_unit.h"

namespace terrier::brain {

class OperatingUnitRecorder : planner::PlanVisitor {
 public:
  OperatingUnitFeatureVector RecordTranslators(const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators);

 private:
  OperatingUnitFeatureType ConvertExpressionType(parser::ExpressionType etype);
  std::unordered_set<OperatingUnitFeatureType> ExtractFeaturesFromExpression(common::ManagedPointer<parser::AbstractExpression> expr);
  void VisitAbstractPlanNode(const planner::AbstractPlanNode *plan);
  void VisitAbstractScanPlanNode(const planner::AbstractScanPlanNode *plan);
  void VisitAbstractJoinPlanNode(const planner::AbstractJoinPlanNode *plan);
  void Visit(const planner::InsertPlanNode *plan) override;
  void Visit(const planner::UpdatePlanNode *plan) override;
  void Visit(const planner::DeletePlanNode *plan) override;
  void Visit(const planner::CSVScanPlanNode *plan) override;
  void Visit(const planner::SeqScanPlanNode *plan) override;
  void Visit(const planner::IndexScanPlanNode *plan) override;
  void Visit(const planner::HashJoinPlanNode *plan) override;
  void Visit(const planner::NestedLoopJoinPlanNode *plan) override;
  void Visit(const planner::LimitPlanNode *plan) override;
  void Visit(const planner::OrderByPlanNode *plan) override;
  void Visit(const planner::ProjectionPlanNode *plan) override;
  void Visit(const planner::AggregatePlanNode *plan) override;

  std::unordered_set<OperatingUnitFeatureType> plan_features_;
};

}  // namespace terrier::brain
