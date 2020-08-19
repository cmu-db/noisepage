#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/cte_scan_provider.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * CteScanLeader Translator
 */
class CteScanLeaderTranslator : public OperatorTranslator, CteScanProvider {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  CteScanLeaderTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
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

 private:
  const planner::CteScanPlanNode *op_;
  //
  //  ast::Identifier GetCteScanIteratorVal();
  //  ast::Identifier GetCteScanIterator();
  // Declare Cte Scan Itarator
  void DeclareCteScanIterator(FunctionBuilder *builder) const;
  //  // Set Column Types for insertion
  //  void SetColumnTypes(FunctionBuilder *builder);
  // Declare the insert PR
  void DeclareInsertPR(FunctionBuilder *builder) const;
  // Get the pr to insert
  void GetInsertPR(FunctionBuilder *builder) const;
  // Fill the insert PR from the child's output
  void FillPRFromChild(WorkContext *context, FunctionBuilder *builder) const;
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder) const;
  ast::Identifier col_oids_var_;
  ast::Identifier col_types_;
  std::vector<int> all_types_;
  ast::Identifier insert_pr_;
  std::vector<catalog::col_oid_t> col_oids_;
  storage::ProjectionMap projection_map_;
  std::unordered_map<std::string, uint32_t> col_name_to_oid_;
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
  //  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  //  void GenPCILoop(FunctionBuilder *builder);
  catalog::Schema schema_;
  StateDescriptor::Entry cte_scan_ptr_entry_;
  StateDescriptor::Entry cte_scan_val_entry_;

  Pipeline build_pipeline_;
  void SetColumnTypes(FunctionBuilder *builder) const;
  void SetColumnOids(FunctionBuilder *builder) const;
};

}  // namespace terrier::execution::compiler
