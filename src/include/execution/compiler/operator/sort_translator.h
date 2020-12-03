#pragma once

#include <vector>

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"

namespace noisepage::planner {
class OrderByPlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

class FunctionBuilder;

/**
 * A translator for order-by plans.
 */
class SortTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a translator for the given order-by plan node.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  SortTranslator(const planner::OrderByPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Define the sort-row structure that's materialized in the sorter.
   * @param decls The top-level declarations.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Define the sorting function.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Define all hook functions
   * @param pipeline Pipeline that helper functions are being generated for.
   * @param decls Query-level declarations.
   */
  void DefineTLSDependentHelperFunctions(const Pipeline &pipeline,
                                         util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Initialize the sorter instance.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the sorter instance.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, initialize the thread-local sorter
   * instance we declared inside.
   * @param pipeline The current pipeline..
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-size and is parallel, destroy the thread-local sorter
   * instance we declared inside.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Implement either the build-side or scan-side of the sort depending on the pipeline this context
   * contains.
   * @param ctx The context of the work.
   * @param function The pipeline function generator.
   */
  void PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const override;

  /**
   * If the given pipeline is for the build-side, we'll need to issue a sort. If the pipeline is
   * parallel, we'll issue a parallel sort. If the sort is only for a top-k, we'll also only issue
   * a top-k sort.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Sorters are never launched in parallel, so this should never occur..
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("Impossible"); }

  /**
   * Sorters are never launched in parallel, so this should never occur.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Impossible");
  }

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * Order-by operators do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Order-by operators do not produce columns from base tables");
  }

  void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Generates start thread-local hook function
   * @param is_sort Whether starting a sort or a merge
   * @returns function decl
   */
  ast::FunctionDecl *GenerateStartTLHookFunction(bool is_sort) const;

  /** Generates end thread-local sort hook function */
  ast::FunctionDecl *GenerateEndTLSortHookFunction() const;

  /** Generates end thread-local merge hook function */
  ast::FunctionDecl *GenerateEndTLMergeHookFunction() const;

  /** Generates end hook in the case where main-thread sorts all thread-local sorters */
  ast::FunctionDecl *GenerateEndSingleSorterHookFunction() const;

 private:
  friend class selfdriving::OperatingUnitRecorder;

  // Check if the given pipelines are build or scan
  bool IsBuildPipeline(const Pipeline &pipeline) const { return &build_pipeline_ == &pipeline; }
  bool IsScanPipeline(const Pipeline &pipeline) const { return GetPipeline() == &pipeline; }

  // Initialize and destroy the given sorter.
  void InitializeSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const;
  void TearDownSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const;

  // Access the attribute at the given index within the provided sort row.
  ast::Expr *GetSortRowAttribute(ast::Identifier sort_row, uint32_t attr_idx) const;

  // Called to scan the global sorter instance.
  void ScanSorter(WorkContext *ctx, FunctionBuilder *function) const;

  // Insert tuple data into the provided sort row.
  void FillSortRow(WorkContext *ctx, FunctionBuilder *function) const;

  // Called to insert the tuple in the context into the sorter instance.
  void InsertIntoSorter(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate comparison function.
  void GenerateComparisonFunction(FunctionBuilder *function);

  // For minirunners.
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

 private:
  // The name of the materialized sort row when inserting into sorter or pulling
  // from an iterator.
  ast::Identifier sort_row_var_;
  ast::Identifier sort_row_type_;
  ast::Identifier lhs_row_, rhs_row_;
  ast::Identifier compare_func_;

  // Build-side pipeline.
  Pipeline build_pipeline_;

  // Where the global and thread-local sorter instances are.
  StateDescriptor::Entry global_sorter_;
  StateDescriptor::Entry local_sorter_;

  enum class CurrentRow { Child, Lhs, Rhs };
  CurrentRow current_row_;

  // For minirunners.
  ast::StructDecl *struct_decl_;

  // The number of rows that are inserted into the sorter.
  StateDescriptor::Entry num_sort_build_rows_;
  // The number of rows that are iterated over by the sorter.
  StateDescriptor::Entry num_sort_iterate_rows_;

  ast::Identifier parallel_starttlsort_hook_fn_;
  ast::Identifier parallel_starttlmerge_hook_fn_;
  ast::Identifier parallel_endtlsort_hook_fn_;
  ast::Identifier parallel_endtlmerge_hook_fn_;
  ast::Identifier parallel_endsinglesorter_hook_fn_;
};

}  // namespace noisepage::execution::compiler
