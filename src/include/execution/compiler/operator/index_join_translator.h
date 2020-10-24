#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/ast/identifier.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"
#include "planner/plannodes/plan_node_defs.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class Schema;
class IndexSchema;
}  // namespace noisepage::catalog

namespace noisepage::planner {
class IndexJoinPlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * Index join translator.
 */
class IndexJoinTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /** Translate IndexJoinPlanNode. */
  IndexJoinTranslator(const planner::IndexJoinPlanNode &plan, CompilationContext *compilation_context,
                      Pipeline *pipeline);

  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(IndexJoinTranslator);

  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override {}

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         that this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  ast::Expr *GetSlotAddress() const override;

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("Index join is serial."); };

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Index join is serial.");
  };

 private:
  void DeclareIterator(FunctionBuilder *builder) const;
  void SetOids(FunctionBuilder *builder) const;
  void FillKey(WorkContext *context, FunctionBuilder *builder, ast::Identifier pr,
               const std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> &index_exprs) const;
  void FreeIterator(FunctionBuilder *builder) const;
  void DeclareIndexPR(FunctionBuilder *builder) const;
  void DeclareTablePR(FunctionBuilder *builder) const;
  void DeclareSlot(FunctionBuilder *builder) const;

 private:
  std::vector<catalog::col_oid_t> input_oids_;
  const catalog::Schema &table_schema_;
  storage::ProjectionMap table_pm_;
  const catalog::IndexSchema &index_schema_;
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm_;

  // Structs and local variables
  ast::Identifier index_iter_;
  ast::Identifier col_oids_;
  ast::Identifier lo_index_pr_;
  ast::Identifier hi_index_pr_;
  ast::Identifier table_pr_;
  ast::Identifier slot_;

  // The size of the index.
  StateDescriptor::Entry index_size_;
  // The number of scans on the index.
  StateDescriptor::Entry num_scans_index_;
  // The number of outer loop iterations.
  StateDescriptor::Entry num_loops_;
};
}  // namespace noisepage::execution::compiler
