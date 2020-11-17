#pragma once

#include <unordered_map>
#include <vector>
#include "execution/compiler/operator/cte_scan_provider.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace noisepage::execution::compiler {

/**
 * CteScanLeader Translator
 */
class CteScanLeaderTranslator : public OperatorTranslator, CteScanProvider {
 public:
  /**
   * Constructor
   * @param plan The ctescanplannode this translator is generating code for
   * @param compilation_context The compilation context being used to translate this node
   * @param pipeline The pipeline this is being translated on
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

  /**
   * @param codegen The codegen object being used in the current context
   * @return A pointer to the cte scan iterator to read from
   */
  ast::Expr *GetCteScanPtr(CodeGen *codegen) const override;

  /**
   * Initialize ctescaniterator
   * @param function The function we are currently building
   */
  void InitializeQueryState(FunctionBuilder *function) const override;

  /**
   * Free the ctescaniterator
   * @param function The cleanup function being built
   */
  void TearDownQueryState(FunctionBuilder *function) const override;

  /**
   * @return This is unreachable as the leader doesn't provide column values
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("No column value expressions should be used in cte leader node");
  }

 private:
  // Declare Cte Scan Itarator
  void DeclareCteScanIterator(FunctionBuilder *builder) const;

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

  catalog::Schema schema_;
  StateDescriptor::Entry cte_scan_ptr_entry_;
  StateDescriptor::Entry cte_scan_val_entry_;

  Pipeline build_pipeline_;

  storage::ColumnMap table_pm_;
  void SetColumnTypes(FunctionBuilder *builder) const;
  void SetColumnOids(FunctionBuilder *builder) const;
};

}  // namespace noisepage::execution::compiler
