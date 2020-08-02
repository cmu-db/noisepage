#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/seq_scan_translator.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * CteScan Translator
 */
class CteScanTranslator : public SeqScanTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  CteScanTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CteScanTranslator);


  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  ast::Expr *TableIterInitExpr() const override;

  catalog::Schema GetPlanSchema() const override;


 private:
  const planner::CteScanPlanNode *op_;
  ast::Expr* GetCteScanIterator() const;
  ast::Identifier col_types_;
  std::vector<int> all_types_;
  ast::Identifier insert_pr_;
  std::vector<catalog::col_oid_t> col_oids_;
  std::unordered_map<std::string, uint32_t> col_name_to_oid_;
  storage::ProjectionMap projection_map_;
  ast::Identifier read_col_oids_;
  ast::Identifier read_tvi_;
  ast::Identifier read_vpi_;
  void SetReadOids(FunctionBuilder *builder) const;
  void DeclareReadTVI(FunctionBuilder *builder) const;
  void GenReadTVIClose(FunctionBuilder *builder) const;
  void DoTableScan(WorkContext *context, FunctionBuilder *builder) const;

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void DeclareSlot(FunctionBuilder *builder) const;
  catalog::Schema schema_;
  ast::Identifier read_slot_;
};

}  // namespace terrier::execution::compiler
