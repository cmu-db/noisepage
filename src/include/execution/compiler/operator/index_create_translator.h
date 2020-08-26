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
   * Does nothing.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Does nothing.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

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
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("index create is serial."); };

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("index create is serial.");
  };

 private:
  // Perform a table scan using the provided table vector iterator pointer.
  void ScanTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate a scan over the VPI.
  void ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const;

  void InitScan(FunctionBuilder *function) const;
  void PrepareContext(WorkContext *context, FunctionBuilder *function) const;

  void CreateIndex(FunctionBuilder *function) const;
  void DeclareInserter(FunctionBuilder *function) const;
  void DeclareIndexPR(FunctionBuilder *function) const;
  void DeclareTablePR(FunctionBuilder *function) const;
  void DeclareTVI(FunctionBuilder *function) const;
  void DeclareSlot(FunctionBuilder *function) const;
  void FillTablePR(FunctionBuilder *function) const;
  void IndexInsert(WorkContext *ctx, FunctionBuilder *function) const;
  void FreeInserter(FunctionBuilder *function) const;

  void SetOids(FunctionBuilder *function) const;
  std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema) const;

  CodeGen *codegen_;
  ast::Identifier inserter_;
  ast::Identifier index_pr_;
  ast::Identifier table_pr_;
  // The name of the declared TVI and VPI.
  ast::Identifier tvi_var_;
  ast::Identifier vpi_var_;
  // The name of the col_oids that the plan wants to scan over.
  ast::Identifier col_oids_var_;

  ast::Identifier slot_var_;

  // Schema of the table that we are inserting on.
  const catalog::Schema &table_schema_;

  // All the oids that we are inserting on.
  std::vector<catalog::col_oid_t> all_oids_;

  // Projection map of the table that we are inserting into.
  // This maps column oids to offsets in a projected row.
  storage::ProjectionMap table_pm_;

  mutable catalog::index_oid_t index_oid_;
};
}  // namespace terrier::execution::compiler
