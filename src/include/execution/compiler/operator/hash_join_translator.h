#pragma once

#include <vector>

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"

namespace terrier::brain {
class OperatingUnitRecorder;
}  // namespace terrier::brain

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

namespace terrier::planner {
class HashJoinPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

class FunctionBuilder;

/**
 * A translator for hash joins.
 */
class HashJoinTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given hash join plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  HashJoinTranslator(const planner::HashJoinPlanNode &plan, CompilationContext *compilation_context,
                     Pipeline *pipeline);

  /**
   * Declare the build-row struct used to materialize tuples from the build side of the join. In the
   * case of left outer joins additionally declare a probe-row struct which lets us
   * materialize tuples from the probe side of the join.
   * @param decls The top-level declarations for the query. The declared structs will be registered
   *              here after they've been constructed.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Only for left outer joins - declare a function joinConsumer which encapsulates the parent translator's
   * functionality
   * @param decls
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
   * Initialize the global hash table.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the global hash table.
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * need to initialize the thread-local join hash table we've declared.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * need to clean up and destroy the thread-local join hash table we've declared.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Implement main join logic. If the context is coming from the left pipeline, the input tuples
   * are materialized into the join hash table. If the context is coming from the right pipeline,
   * the input tuples are probed in the join hash table.
   * @param ctx The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const override;

  /**
   * If the pipeline context represents the left pipeline and the left pipeline is parallel, we'll
   * issue a parallel join hash table construction at this point.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * Hash-joins do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Hash-joins do not produce columns from base tables.");
  }

  void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

 private:
  friend class brain::OperatingUnitRecorder;

  // Is the given pipeline this join's left pipeline?
  bool IsLeftPipeline(const Pipeline &pipeline) const { return &left_pipeline_ == &pipeline; }

  // Is the given pipeline this join's right pipeline?
  bool IsRightPipeline(const Pipeline &pipeline) const { return GetPipeline() == &pipeline; }

  // Initialize the given join hash table instance, provided as a *JHT.
  void InitializeJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const;

  // Clean up and destroy the given join hash table instance, provided as a *JHT.
  void TearDownJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const;

  // Access an attribute at the given index in the provided row.
  ast::Expr *GetRowAttribute(ast::Expr *row, uint32_t attr_idx) const;

  // Evaluate the provided hash keys in the provided context and return the
  // results in the provided results output vector.
  ast::Expr *HashKeys(WorkContext *ctx, FunctionBuilder *function,
                      const std::vector<common::ManagedPointer<parser::AbstractExpression>> &hash_keys) const;

  // Fill the build row with the columns from the given context.
  void FillBuildRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *build_row) const;

  // Fill the probe row with the columns from the given context.
  void FillProbeRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *probe_row) const;

  // Input the tuple(s) in the provided context into the join hash table.
  void InsertIntoJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Probe the join hash table with the input tuple(s).
  void ProbeJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Check the right mark.
  void CheckRightMark(WorkContext *ctx, FunctionBuilder *function, ast::Identifier right_mark) const;

  // Check the join predicate.
  void CheckJoinPredicate(WorkContext *ctx, FunctionBuilder *function) const;

  // Only for left outer joins - iterate the hash table and output unmatched left rows
  void CollectUnmatchedLeftRows(FunctionBuilder *function) const;

  /** @return The struct that was declared, used for the minirunner. */
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

  /** Generate start hook function for parallel build */
  ast::FunctionDecl *GenerateStartHookFunction() const;

  /** Generate end hook function for parallel build */
  ast::FunctionDecl *GenerateEndHookFunction() const;

 private:
  // Flag to indicate whether or not we are in the joinConsumer function
  bool join_consumer_flag_;

  // The name of the materialized row when inserting into join hash table.
  ast::Identifier build_row_var_;
  ast::Identifier build_row_type_;
  // For mark-based joins.
  ast::Identifier build_mark_;

  // The name of the materialized probe row
  ast::Identifier probe_row_var_;
  ast::Identifier probe_row_type_;

  // The name of the function which encapuslates the join conumser
  ast::Identifier join_consumer_;

  // The left build-side pipeline.
  Pipeline left_pipeline_;

  // The slots in the global and thread-local state where this join's join hash
  // table is stored.
  StateDescriptor::Entry global_join_ht_;
  StateDescriptor::Entry local_join_ht_;

  // The number of rows that are inserted into the hash table.
  StateDescriptor::Entry num_build_rows_;
  // The number of probes that are performed.
  StateDescriptor::Entry num_probe_rows_;
  // The number of rows that are matched by the probes.
  StateDescriptor::Entry num_match_rows_;

  // Struct declaration for minirunner.
  ast::StructDecl *struct_decl_;

  ast::Identifier parallel_build_pre_hook_fn_;
  ast::Identifier parallel_build_post_hook_fn_;
};

}  // namespace terrier::execution::compiler
