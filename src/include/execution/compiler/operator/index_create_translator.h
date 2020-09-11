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
   * Initialize the global storage interface.
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Tear-down the global storage interface.
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
   * Implement create index logic where it fills in the scanned tuples obtained from the StorageInterface struct
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return The child's output at the given index.
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override {
    UNREACHABLE("index create doesn't have child");
  };

  /**
   * @return An expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Not implemented"); };

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override;

 private:
  void InitializeStorageInterface(FunctionBuilder *function, ast::Expr *storage_interface_ptr) const;

  void TearDownStorageInterface(FunctionBuilder *function, ast::Expr *storage_interface_ptr) const;

  void SetGlobalOids(FunctionBuilder *function, ast::Expr *global_col_oids) const;

  void DeclareIndexPR(FunctionBuilder *function) const;
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
};
}  // namespace terrier::execution::compiler
