#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "common/managed_pointer.h"
#include "execution/ast/ast.h"
#include "execution/ast/context.h"
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
   * Constructor
   * @param accessor CatalogAccessor
   * @param ast_ctx AstContext
   */
  explicit OperatingUnitRecorder(common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                 common::ManagedPointer<execution::ast::Context> ast_ctx)
      : accessor_(accessor), ast_ctx_(ast_ctx) {}

  /**
   * Extracts features from OperatorTranslators
   * @param translators Vector of OperatorTranslators to extract from
   * @returns Vector of extracted features (ExecutionOperatingUnitFeature)
   */
  ExecutionOperatingUnitFeatureVector RecordTranslators(
      const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators);

 private:
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
   * Accumulate Feature Information
   * @param type Type
   * @param key_size Key size
   * @param num_keys Number keys
   * @param plan Plan Node
   * @param scaling_factor Scaling factor
   * @param mem_factor Memory scaling factor
   */
  void AggregateFeatures(brain::ExecutionOperatingUnitType type, size_t key_size, size_t num_keys,
                         const planner::AbstractPlanNode *plan, size_t scaling_factor, double mem_factor);

  /**
   * Compute Memory Scaling Factor against the mini-runners
   * @param decl Struct being used by the pipeline
   * @param total_offset Additional size by the feature (i.e HashTableEntry)
   * @param key_size Key Size
   * @param ref_offset Additional size to be added to the reference size
   * @returns scaling factor
   */
  double ComputeMemoryScaleFactor(execution::ast::StructDecl *decl, size_t total_offset, size_t key_size,
                                  size_t ref_offset);

  /**
   * Compute key size from vector of expressions
   * @param exprs Expressions
   * @returns key size
   */
  size_t ComputeKeySize(const std::vector<common::ManagedPointer<parser::AbstractExpression>> &exprs);

  /**
   * Compute key size from output schema
   * @param plan Plan
   * @returns key size
   */
  size_t ComputeKeySizeOutputSchema(const planner::AbstractPlanNode *plan);

  /**
   * Compute key size from output schema
   * @param tbl_oid Table OID
   * @returns key size
   */
  size_t ComputeKeySize(catalog::table_oid_t tbl_oid);

  /**
   * Compute key size from vector of column oids
   * @param tbl_oid Table OID
   * @param cols vector of column oids
   */
  size_t ComputeKeySize(catalog::table_oid_t tbl_oid, const std::vector<catalog::col_oid_t> &cols);

  /**
   * Compute key size from vector of index oids
   * @param idx_oid Index OID
   * @param cols index column oids
   * @returns key size
   */
  size_t ComputeKeySize(catalog::index_oid_t idx_oid, const std::vector<catalog::indexkeycol_oid_t> &cols);

  /**
   * Record arithmetic features
   * @param plan Plan
   * @param scaling Scaling Factor
   */
  void RecordArithmeticFeatures(const planner::AbstractPlanNode *plan, size_t scaling);

  /**
   * Current Translator Feature Type
   */
  ExecutionOperatingUnitType plan_feature_type_;

  /**
   * Current Translator
   */
  common::ManagedPointer<execution::compiler::OperatorTranslator> current_translator_;

  /**
   * Arithmetic features for a given plan
   */
  std::vector<std::pair<type::TypeId, ExecutionOperatingUnitType>> arithmetic_feature_types_;

  /**
   * Structure for storing features of a pipeline
   */
  std::unordered_multimap<ExecutionOperatingUnitType, ExecutionOperatingUnitFeature> pipeline_features_;

  /**
   * CatalogAccessor
   */
  common::ManagedPointer<catalog::CatalogAccessor> accessor_;

  /**
   * AstContext
   */
  common::ManagedPointer<execution::ast::Context> ast_ctx_;
};

}  // namespace terrier::brain
