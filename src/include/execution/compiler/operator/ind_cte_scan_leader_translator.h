#pragma once

#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/cte_scan_provider.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace noisepage::execution::compiler {

/**
 * IndCteScanLeader Translator
 */
class IndCteScanLeaderTranslator : public OperatorTranslator, CteScanProvider {
 public:
  /**
   * Constructor
   * @param plan The ctescanplanndoe this translator is generating code for
   * @param compilation_context The compilation context being used to translate this node
   * @param pipeline The pipeline this is being translated on
   */
  IndCteScanLeaderTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
                             Pipeline *pipeline);

  /**
   * Does nothing
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Get tuples from children and insert into indctescaniterator
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @param codegen The codegen object being used in the current context
   * @return A pointer to the cte scan iterator to read from
   */
  ast::Expr *GetCteScanPtr(CodeGen *codegen) const override;

  /**
   * Initialize the indctescaniterator
   * @param function The initialzation function being built
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Free the indctescaniterator
   * @param function The cleanup function being built
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * @return Leader nodes shouldn't provide column value expressions
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("No column value expressions should be used in cte leader node");
  }

 private:
  void PopulateReadCteScanIterator(FunctionBuilder *builder) const;

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
  catalog::Schema schema_;
  StateDescriptor::Entry cte_scan_ptr_entry_;
  StateDescriptor::Entry cte_scan_val_entry_;

  Pipeline base_pipeline_;
  Pipeline build_pipeline_;
  void SetColumnTypes(FunctionBuilder *builder) const;
  void SetColumnOids(FunctionBuilder *builder) const;
  void DeclareIndCteScanIterator(FunctionBuilder *builder) const;
  void GenInductiveLoop(WorkContext *context, FunctionBuilder *builder) const;
  void FinalizeReadCteScanIterator(FunctionBuilder *builder) const;
};

}  // namespace noisepage::execution::compiler
