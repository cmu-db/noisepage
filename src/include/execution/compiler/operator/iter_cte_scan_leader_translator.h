#pragma once

#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/cte_scan_provider.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * IterCteScanLeader Translator
 */
class IterCteScanLeaderTranslator : public OperatorTranslator, CteScanProvider {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  IterCteScanLeaderTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
                              Pipeline *pipeline);

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Get tuples from children and insert into ctescaniterator
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  ast::Expr *GetCteScanPtr(CodeGen *codegen) const override;

  void InitializeQueryState(FunctionBuilder *function) const override;

  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("No column value expressions should be used in cte leader node");
  }

  void PopulateReadCteScanIterator(FunctionBuilder *builder) const;

 private:
  const planner::CteScanPlanNode *op_;
  //
  //  ast::Identifier GetCteScanIteratorVal();
  //  ast::Identifier GetCteScanIterator();
  // Declare Cte Scan Itarator
  //  // Set Column Types for insertion
  //  void SetColumnTypes(FunctionBuilder *builder);
  // Declare the insert PR
  void DeclareInsertPR(FunctionBuilder *builder) const;
  // Get the pr to insert
  void GetInsertPR(FunctionBuilder *builder) const;
  // Fill the insert PR from the child's output
  void FillPRFromChild(WorkContext *context, FunctionBuilder *builder, uint32_t child_idx) const;
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder) const;
  ast::Identifier col_types_;
  ast::Identifier col_oids_var_;
  ast::Identifier insert_pr_;
  std::vector<catalog::col_oid_t> col_oids_;
  //  void SetReadOids(FunctionBuilder *builder);
  //  void DeclareReadTVI(FunctionBuilder *builder);
  //  void GenReadTVIClose(FunctionBuilder *builder);
  //  void DoTableScan(FunctionBuilder *builder);
  //
  //  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  //  void GenTVILoop(FunctionBuilder *builder);
  //
  //  void DeclarePCI(FunctionBuilder *builder);
  //  void DeclareSlot(FunctionBuilder *builder);
  //
  //  // var pci = @tableIterGetPCI(&tvi)
  //  // for (; @pciHasNext(pci); @pciAdvance(pci))n {...}
  //  void GenPCILoop(FunctionBuilder *builder);
  catalog::Schema schema_;
  StateDescriptor::Entry cte_scan_ptr_entry_;
  StateDescriptor::Entry cte_scan_val_entry_;

  Pipeline base_pipeline_;
  Pipeline build_pipeline_;
  void SetColumnTypes(FunctionBuilder *builder) const;
  void SetColumnOids(FunctionBuilder *builder) const;
  void DeclareIterCteScanIterator(FunctionBuilder *builder) const;
  void GenInductiveLoop(WorkContext *context, FunctionBuilder *builder) const;
  void FinalizeReadCteScanIterator(FunctionBuilder *builder) const;
};

}  // namespace terrier::execution::compiler
