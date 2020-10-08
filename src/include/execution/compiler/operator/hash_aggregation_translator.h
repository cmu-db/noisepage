#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"

namespace terrier::planner {
class AggregatePlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

class FunctionBuilder;

/**
 * A translator for hash-based aggregations.
 */
class HashAggregationTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given aggregation plan.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  HashAggregationTranslator(const planner::AggregatePlanNode &plan, CompilationContext *compilation_context,
                            Pipeline *pipeline);

  /**
   * Define the aggregation row structure.
   * @param decls Where the defined structure will be registered.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * If the build-pipeline is parallel, we'll need to define the partition-merging function.
   * @param decls Where the defined functions will be registered.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Initialize the global aggregation hash table.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Destroy the global aggregation hash table.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * Initialize the thread-local aggregation hash table, if needed.
   * @param pipeline Current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Tear-down and destroy the thread-local aggregation hash table, if needed.
   * @param pipeline Current pipeline.
   * @param function The pipeline generating function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * If the context pipeline is for the build-side, we'll aggregate the input into the aggregation
   * hash table. Otherwise, we'll perform a scan over the resulting aggregates in the aggregation
   * hash table.
   * @param context The context.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * If the provided context is for the build pipeline and we're performing a parallel aggregation,
   * then we'll need to move thread-local aggregation hash table partitions into the main
   * aggregation hash table.
   * @param pipeline Current pipeline.
   * @param function The pipeline generating function.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * We'll issue a parallel partitioned scan over the aggregation hash table. In this case, the
   * last argument to the worker function will be the aggregation hash table we're scanning.
   * @return The set of additional worker parameters.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * If the aggregation is parallelized, we'll launch ara parallel partitioned scan over the
   * aggregation hash table.
   * @param function The pipeline generating function.
   * @param work_func_name The name of the worker function to invoke.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * Hash-based aggregations do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Hash-based aggregations do not produce columns from base tables.");
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

  // Declare the payload and input structures. Called from DefineHelperStructs().
  ast::StructDecl *GeneratePayloadStruct();
  ast::StructDecl *GenerateInputValuesStruct();

  // Generate the overflow partition merging process.
  ast::FunctionDecl *GenerateKeyCheckFunction();
  ast::FunctionDecl *GeneratePartialKeyCheckFunction();
  ast::FunctionDecl *GenerateMergeOverflowPartitionsFunction();
  void MergeOverflowPartitions(FunctionBuilder *function, ast::Expr *agg_ht, ast::Expr *iter);

  // Initialize and destroy the input aggregation hash table. These are called
  // from InitializeQueryState() and InitializePipelineState().
  void InitializeAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const;
  void TearDownAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const;

  // Access an attribute at the given index in the provided aggregate row.
  ast::Expr *GetGroupByTerm(ast::Identifier agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTerm(ast::Identifier agg_row, uint32_t attr_idx) const;
  ast::Expr *GetAggregateTermPtr(ast::Identifier agg_row, uint32_t attr_idx) const;

  // These functions define steps in the "build" phase of the aggregation.
  // 1. Filling input values.
  // 2. Probing aggregation hash table.
  //   2a. Hashing input.
  //   2b. Performing lookup.
  // 3. Initializing new aggregates.
  // 4. Advancing existing aggregates.
  ast::Identifier FillInputValues(FunctionBuilder *function, WorkContext *ctx) const;
  ast::Identifier HashInputKeys(FunctionBuilder *function, ast::Identifier agg_values) const;
  ast::Identifier PerformLookup(FunctionBuilder *function, ast::Expr *agg_ht, ast::Identifier hash_val,
                                ast::Identifier agg_values) const;
  void ConstructNewAggregate(FunctionBuilder *function, ast::Expr *agg_ht, ast::Identifier agg_payload,
                             ast::Identifier agg_values, ast::Identifier hash_val) const;
  void AdvanceAggregate(FunctionBuilder *function, ast::Identifier agg_payload, ast::Identifier agg_values) const;

  // Merge the input row into the aggregation hash table.
  void UpdateAggregates(WorkContext *context, FunctionBuilder *function, ast::Expr *agg_ht) const;

  // Scan the final aggregation hash table.
  void ScanAggregationHashTable(WorkContext *context, FunctionBuilder *function, ast::Expr *agg_ht) const;

  // For minirunners.
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

  /** Generate start hook function for parallel merge */
  ast::FunctionDecl *GenerateStartHookFunction() const;

  /** Generate end hook function for parallel merge */
  ast::FunctionDecl *GenerateEndHookFunction() const;

 private:
  friend class brain::OperatingUnitRecorder;
  // The name of the variable used to:
  // 1. Materialize an input row and insert into the aggregation hash table.
  // 2. Read from an iterator when iterating over all aggregates.
  ast::Identifier agg_row_var_;
  // The names of the payload and input values struct.
  ast::Identifier agg_payload_type_;
  ast::Identifier agg_values_type_;
  // The names of the full key-check function, the partial key check function
  // and the overflow partition merging functions, respectively.
  ast::Identifier key_check_fn_;
  ast::Identifier key_check_partial_fn_;
  ast::Identifier merge_partitions_fn_;

  // The build pipeline.
  Pipeline build_pipeline_;

  // The global and thread-local aggregation hash tables.
  StateDescriptor::Entry global_agg_ht_;
  StateDescriptor::Entry local_agg_ht_;

  // For minirunners
  ast::StructDecl *struct_decl_;

  // The number of input rows to the aggregation.
  StateDescriptor::Entry num_agg_inputs_;

  // The number of output rows from the aggregation.
  StateDescriptor::Entry num_agg_outputs_;

  // TBB can run multiple tasks using the same thread local state. For counter
  // recording, each task will record an estimation of the "number of unique
  // entries" inserted into the aggregation hash table during that task.
  //
  // agg_count_ is thus used to track the number of "uniquely" inserted tuples
  // at the end of the previous task invocation with the same thread local state.
  //
  // agg_count_ is thus initialized only in InitializePipelineState. Counters
  // initialized by InitializeCounters() are "reset" to their initial value
  // at the start of the task invocation's work function -- however, agg_count_
  // cannot be reset and so is initialized separately.
  //
  // The general pattern for agg_count_ is as follows:
  //    while (work to be done.)
  //      - Insert work's data into aggregation hash table
  //      - Record AggHashTableGetInsertCount() - agg_count_
  //      - agg_count_ = AggHashTableGetInsertCount()
  //
  StateDescriptor::Entry agg_count_;

  ast::Identifier parallel_build_pre_hook_fn_;
  ast::Identifier parallel_build_post_hook_fn_;
};

}  // namespace terrier::execution::compiler
