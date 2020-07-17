#pragma once

#include <vector>

#include "catalog/schema.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::planner {
class InsertPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

/**
 * InsertTranslator
 */
class InsertTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given insert plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Does nothing
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Does nothing
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Implement insertion logic where it fills in the insert PR obtained from the StorageInterface struct
   * with values from the child
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return thfe child's output at the given index
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:

  // Declare storageinterface
  void DeclareInserter(FunctionBuilder *builder) const;

  // free the storageinterface
  void GenInserterFree(FunctionBuilder *builder) const;

  // sets the oids we are inserting on using schema from the insert plan node
  void SetOids(FunctionBuilder *builder) const;

  // declares the projected row that we will be using the insert values with
  void DeclareInsertPR(FunctionBuilder *builder) const;

  // gets the projected row pointer that we will fill in with values to insert
  void GetInsertPR(FunctionBuilder *builder) const;

  // sets the values in the projected row we will use to insert into the table
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const;

  // insert into the table
  void GenTableInsert(FunctionBuilder *builder) const;

  // insert into all indexes of this table
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

  // gets all the column oids in a schema
  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

  // storageinterface inserter struct we use to insert
  ast::Identifier inserter_;

  // projected row that the inserter spits out for us to insert with
  ast::Identifier insert_pr_;

  // column oid's we are inserting on
  ast::Identifier col_oids_;

  // schema of the table we are inserting on
  const catalog::Schema &table_schema_;

  // all oid's we are inserting on
  std::vector<catalog::col_oid_t> all_oids_;

  // projected map to tell us the offsets of columns in a row in our table we are
  // inserting on
  storage::ProjectionMap table_pm_;
};

}  // namespace terrier::execution::compiler
