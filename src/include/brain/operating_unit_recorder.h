#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "brain/operating_unit.h"
#include "common/managed_pointer.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::brain {

/**
 * OperatingUnitRecorder extracts all relevant ExecutionOperatingUnitFeature
 * from a given vector of OperatorTranslators.
 */
class OperatingUnitRecorder : planner::PlanVisitor {
 public:
  /**
   * Extracts features from OperatorTranslators
   * @param translators Vector of OperatorTranslators to extract from
   * @returns Vector of extracted features (ExecutionOperatingUnitFeature)
   */
  ExecutionOperatingUnitFeatureVector RecordTranslators(
      const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators);

 private:
  /**
   * Derive the type of computation
   * @param expr Expression
   * @returns type of computation
   */
  type::TypeId DeriveComputation(common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Converts a expression to brain::ExecutionOperatingUnitType
   *
   * Function returns brain::ExecutionOperatingUnitType::INVALID if the
   * parser::ExpressionType does not have an equivalent conversion.
   *
   * @param expr Expression
   * @returns converted equivalent brain::ExecutionOperatingUnitType
   */
  ExecutionOperatingUnitType ConvertExpressionType(common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Extracts features from an expression into a vector
   * @param expr Expression to extract features from
   * @return vector of extracted features
   */
  std::vector<ExecutionOperatingUnitType> ExtractFeaturesFromExpression(
      common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Handle additional processing for AbstractPlanNode
   * @param plan plan to process
   */
  void VisitAbstractPlanNode(const planner::AbstractPlanNode *plan);

  /**
   * Handle additional processing for AbstractScanPlanNode
   * @param plan plan to process
   */
  void VisitAbstractScanPlanNode(const planner::AbstractScanPlanNode *plan);

  /**
   * Handle additional processing for AbstractJoinPlanNode
   * @param plan plan to process
   */
  void VisitAbstractJoinPlanNode(const planner::AbstractJoinPlanNode *plan);
  void Visit(const planner::InsertPlanNode *plan) override;
  void Visit(const planner::UpdatePlanNode *plan) override;
  void Visit(const planner::DeletePlanNode *plan) override;
  void Visit(const planner::CSVScanPlanNode *plan) override;
  void Visit(const planner::SeqScanPlanNode *plan) override;
  void Visit(const planner::IndexScanPlanNode *plan) override;
  void Visit(const planner::IndexJoinPlanNode *plan) override;
  void Visit(const planner::HashJoinPlanNode *plan) override;
  void Visit(const planner::NestedLoopJoinPlanNode *plan) override;
  void Visit(const planner::LimitPlanNode *plan) override;
  void Visit(const planner::OrderByPlanNode *plan) override;
  void Visit(const planner::ProjectionPlanNode *plan) override;
  void Visit(const planner::AggregatePlanNode *plan) override;

  /**
   * Feature of plan currently being visited
   */
  ExecutionOperatingUnitType plan_feature_;

  /**
   * Structure used to store features for a single plan visit
   */
  std::vector<ExecutionOperatingUnitType> plan_features_;
};

}  // namespace terrier::brain
