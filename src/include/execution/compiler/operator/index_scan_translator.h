#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/index_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Index scan translator.
 */
class IndexScanTranslator : public OperatorTranslator {
 public:
  /** Translate IndexScanPlanNode. */
  IndexScanTranslator(const planner::IndexScanPlanNode &plan, CompilationContext *compilation_context,
                      Pipeline *pipeline);

  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(IndexScanTranslator);

  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override {}

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         that this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  ast::Expr *GetSlotAddress() const override;

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
  ast::Identifier index_pr_;
  ast::Identifier lo_index_pr_;
  ast::Identifier hi_index_pr_;
  ast::Identifier table_pr_;
  ast::Identifier slot_;
};
}  // namespace terrier::execution::compiler
