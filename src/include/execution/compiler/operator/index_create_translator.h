#pragma once

#include <vector>

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"
#include "storage/storage_defs.h"

namespace terrier::catalog {
class Schema;
}  // namespace terrier::catalog

namespace terrier::planner {
class CreateIndexPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

class FunctionBuilder;

/**
 * A translator for sequential table scans.
 */
class IndexCreateTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  IndexCreateTranslator(const planner::CreateIndexPlanNode &plan, CompilationContext *compilation_context,
                        Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(IndexCreateTranslator);

  /**
   * Initialize the global col_oids array.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Nothing to tear down.
   */
  void TearDownQueryState(FunctionBuilder *function) const override{};

  /**
   * Initilize a thread local storage interface and index pr, work for both serial and parallel
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * clean up the storage interface, where destructor of index pr also gets called
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Define all hook functions
   * @param pipeline Pipeline that helper functions are being generated for.
   * @param decls Query-level declarations.
   */
  void DefineTLSDependentHelperFunctions(const Pipeline &pipeline,
                                         util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Implement create index logic where it fills in the scanned tuples obtained from the StorageInterface struct
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /** @return This translator doesn't have a child */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override {
    UNREACHABLE("index create doesn't have child");
  };

  /** @return Not implemented */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Not implemented"); };

  /** @return a collection of parameters for the scan function */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /** Launch work for parallel execution */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override;

  void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /** Generates a EndHook function to be invoked after parallel create index has finished */
  ast::FunctionDecl *GenerateEndHookFunction() const;

 private:
  // Initialization for serial and parallel
  void SetGlobalOids(FunctionBuilder *function, ast::Expr *global_col_oids) const;
  void InitializeStorageInterface(FunctionBuilder *function, ast::Expr *storage_interface_ptr) const;
  void TearDownStorageInterface(FunctionBuilder *function, ast::Expr *storage_interface_ptr) const;
  void DeclareIndexPR(FunctionBuilder *function) const;

  // Initialization for serial only
  void DeclareTVI(FunctionBuilder *function) const;

  // Perform a table scan using the provided table vector iterator pointer.
  void ScanTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate a scan over the VPI.
  void ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const;
  void IndexInsert(WorkContext *ctx, FunctionBuilder *function) const;

  std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema) const;

  CodeGen *codegen_;

  // The name of the col_oids that the plan wants to scan over.
  StateDescriptor::Entry global_col_oids_;
  // thread local storage interface
  StateDescriptor::Entry local_storage_interface_;
  // thread local index pr
  StateDescriptor::Entry local_index_pr_;
  // thread local tuple slot
  StateDescriptor::Entry local_tuple_slot_;

  // The name of the declared TVI and VPI.
  ast::Identifier tvi_var_;
  ast::Identifier vpi_var_;

  // The name of the declared slot.
  ast::Identifier slot_var_;
  catalog::table_oid_t table_oid_;
  // Schema of the table that we are inserting on.
  const catalog::Schema &table_schema_;

  // All the oids that we are inserting on.
  std::vector<catalog::col_oid_t> all_oids_;

  catalog::index_oid_t index_oid_;

  // The number of rows that are inserted.
  StateDescriptor::Entry num_inserts_;

  ast::Identifier parallel_build_post_hook_fn_;
};
}  // namespace terrier::execution::compiler
