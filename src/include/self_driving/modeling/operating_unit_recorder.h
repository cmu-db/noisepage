#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "planner/plannodes/plan_visitor.h"
#include "self_driving/modeling/operating_unit.h"
#include "type/type_id.h"

namespace noisepage::catalog {
class CatalogAccessor;
class IndexSchema;
}  // namespace noisepage::catalog

namespace noisepage::execution {
namespace ast {
class Context;
class StructDecl;
}  // namespace ast
namespace compiler {
class OperatorTranslator;
class Pipeline;
}  // namespace compiler
}  // namespace noisepage::execution

namespace noisepage::parser {
class AbstractExpression;
}

namespace noisepage::planner {
class AbstractPlanNode;
class AbstractJoinPlanNode;
class AbstractScanPlanNode;
}  // namespace noisepage::planner

namespace noisepage::selfdriving {

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
   * @param pipeline Current pipeline, used to figure out if a given translator is Build or Probe.
   * @param query_text The SQL query string
   */
  explicit OperatingUnitRecorder(common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                 common::ManagedPointer<execution::ast::Context> ast_ctx,
                                 common::ManagedPointer<execution::compiler::Pipeline> pipeline,
                                 common::ManagedPointer<const std::string> query_text)
      : accessor_(accessor), ast_ctx_(ast_ctx), current_pipeline_(pipeline) {}

  /**
   * Extracts features from OperatorTranslators
   * @param translators Vector of OperatorTranslators to extract from
   * @return Vector of extracted features (ExecutionOperatingUnitFeature)
   */
  ExecutionOperatingUnitFeatureVector RecordTranslators(
      const std::vector<execution::compiler::OperatorTranslator *> &translators);

  /**
   * Adjust key_size and num_key based on type
   *
   * @param type Type
   * @param key_size
   * @param num_key
   */
  static void AdjustKeyWithType(type::TypeId type, size_t *key_size, size_t *num_key);

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
  void Visit(const planner::CreateIndexPlanNode *plan) override;

  /**
   * Records the index operations performed by a plan node
   * that requires modifying any system index.
   *
   * @param index_oids Index OIDs to record operations for.
   */
  template <typename IndexPlanNode>
  void RecordIndexOperations(const std::vector<catalog::index_oid_t> &index_oids);

  template <typename Translator>
  void RecordAggregateTranslator(common::ManagedPointer<Translator> translator, const planner::AggregatePlanNode *plan);

  /**
   * Accumulate Feature Information
   * @param type Type
   * @param key_size Key size
   * @param num_keys Number keys
   * @param plan Plan Node
   * @param scaling_factor Scaling factor
   * @param mem_factor Memory scaling factor
   */
  void AggregateFeatures(selfdriving::ExecutionOperatingUnitType type, size_t key_size, size_t num_keys,
                         const planner::AbstractPlanNode *plan, size_t scaling_factor, double mem_factor);

  /**
   * Compute Memory Scaling Factor against the mini-runners
   * @param decl Struct being used by the pipeline
   * @param total_offset Additional size by the feature (i.e HashTableEntry)
   * @param key_size Key Size
   * @param ref_offset Additional size to be added to the reference size
   * @return scaling factor
   */
  double ComputeMemoryScaleFactor(execution::ast::StructDecl *decl, size_t total_offset, size_t key_size,
                                  size_t ref_offset);

  /**
   * Compute key size from vector of expressions
   * @param exprs Expressions
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySize(const std::vector<common::ManagedPointer<parser::AbstractExpression>> &exprs, size_t *num_key);

  /**
   * Compute key size from output schema
   * @param plan Plan
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySizeOutputSchema(const planner::AbstractPlanNode *plan, size_t *num_key);

  /**
   * Compute key size from output schema
   * @param tbl_oid Table OID
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySize(catalog::table_oid_t tbl_oid, size_t *num_key);

  /**
   * Compute key size from vector of column oids
   * @param tbl_oid Table OID
   * @param cols vector of column oids
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySize(catalog::table_oid_t tbl_oid, const std::vector<catalog::col_oid_t> &cols, size_t *num_key);

  /**
   * Compute key size from an IndexSchema and optional col specifiers
   * @param schema Index Schema
   * @param restrict_cols whether to consider cols vector
   * @param cols Set of column specifiers to look at
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySize(common::ManagedPointer<const catalog::IndexSchema> schema, bool restrict_cols,
                        const std::vector<catalog::indexkeycol_oid_t> &cols, size_t *num_key);

  /**
   * Compute key size from vector of index oids
   * @param idx_oid Index OID
   * @param cols index column oids
   * @param num_key Number of keys
   * @return key size
   */
  size_t ComputeKeySize(catalog::index_oid_t idx_oid, const std::vector<catalog::indexkeycol_oid_t> &cols,
                        size_t *num_key);

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

  /** Pipeline, used to figure out if current translator is Build or Probe. */
  common::ManagedPointer<execution::compiler::Pipeline> current_pipeline_;
};

}  // namespace noisepage::selfdriving
