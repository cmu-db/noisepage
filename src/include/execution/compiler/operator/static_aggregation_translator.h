#pragma once

#include <unordered_map>
#include <vector>

#include "execution/compiler/operator/distinct_aggregation_util.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"

namespace noisepage::planner {
class AggregatePlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

class FunctionBuilder;

/**
 * A translator for static aggregations.
 */
class StaticAggregationTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given static aggregation  plan. The translator occurs within
   * the provided compilation context, and the operator is a step in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  StaticAggregationTranslator(const planner::AggregatePlanNode &plan, CompilationContext *compilation_context,
                              Pipeline *pipeline);

  /**
   * Declare the aggregation structure.
   * @param decls Query-level declarations.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * When parallel, generate the partial aggregate merging function.
   * @param decls Query-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * If the provided pipeline is the build-side, initialize the declare partial aggregate.
   * @param pipeline The pipeline whose state is being initialized.
   * @param function The function being built.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Initialize the global aggregation hash table.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Destroy the global aggregation hash table.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * Before the pipeline begins, initial the partial aggregates.
   * @param pipeline The pipeline whose pre-work logic is being generated.
   * @param function The function being built.
   */
  void BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Main aggregation logic.
   * @param context The context of the work.
   * @param function The function being built.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Finish the provided pipeline.
   * @param pipeline The pipeline whose post-work logic is being generated.
   * @param function The function being built.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("Static aggregations are never launched in parallel");
  }

  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Static aggregations are never launched in parallel");
  }

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Static aggregations do not produce columns from base tables.");
  }

  void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

 private:
  // Access the plan.
  const planner::AggregatePlanNode &GetAggPlan() const { return GetPlanAs<planner::AggregatePlanNode>(); }

  // Check if the input pipeline is either the build-side or producer-side.
  bool IsBuildPipeline(const Pipeline &pipeline) const { return &build_pipeline_ == &pipeline; }
  bool IsProducePipeline(const Pipeline &pipeline) const { return GetPipeline() == &pipeline; }

  ast::Expr *GetAggregateTerm(ast::Expr *agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTermPtr(ast::Expr *agg_row, uint32_t attr_idx) const;

  ast::StructDecl *GeneratePayloadStruct();
  ast::StructDecl *GenerateValuesStruct();

  void InitializeAggregates(FunctionBuilder *function, bool local) const;

  void UpdateGlobalAggregate(WorkContext *ctx, FunctionBuilder *function) const;

  // For minirunners.
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

 private:
  friend class selfdriving::OperatingUnitRecorder;

  ast::Identifier agg_row_var_;
  ast::Identifier agg_payload_type_;
  ast::Identifier agg_values_type_;

  // The name of the merging function.
  ast::Identifier merge_func_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // States.
  StateDescriptor::Entry global_aggs_;
  StateDescriptor::Entry local_aggs_;

  // For minirunners
  ast::StructDecl *struct_decl_;

  // For distinct aggregations
  std::unordered_map<size_t, DistinctAggregationFilter> distinct_filters_;
  // The number of input rows to the aggregation.
  StateDescriptor::Entry num_agg_inputs_;

  // The number of output rows from the aggregation.
  StateDescriptor::Entry num_agg_outputs_;
};

}  // namespace noisepage::execution::compiler
