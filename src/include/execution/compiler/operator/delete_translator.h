#pragma once

#include <vector>

#include "catalog/schema.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/delete_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Delete Translator
 */
class DeleteTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  DeleteTranslator(const planner::DeletePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Initialize the FilterManager if required.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Generate the scan.
   * @param context The context of the work.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return the child's output at the given index
   */
  //   ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Delete doesn't provide values"); }

  // Produce and consume logic
  //  void Produce(FunctionBuilder *builder) override;
  //  void Abort(FunctionBuilder *builder) override;
  //  void Consume(FunctionBuilder *builder) override;

  //  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Deletes don't output anything"); };

  //  const planner::AbstractPlanNode *Op() override { return op_; }

  //  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the deleter
  void DeclareDeleter(FunctionBuilder *builder) const;
  // Free the deleter
  void GenDeleterFree(FunctionBuilder *builder) const;
  // Set the oids variable
  void SetOids(FunctionBuilder *builder) const;
  // Delete from table.
  void GenTableDelete(WorkContext *context, FunctionBuilder *builder) const;
  // Delete from index.
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;
  // Get all columns oids.
  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  ast::Identifier deleter_;
  ast::Identifier col_oids_;

  // TODO(Amadou): If tpl supports null arrays, leave this empty. Otherwise, put a dummy value of 1 inside.
  std::vector<catalog::col_oid_t> oids_;
};

}  // namespace terrier::execution::compiler
