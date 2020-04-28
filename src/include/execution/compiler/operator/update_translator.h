#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/util/execution_common.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace catalog {
class Schema;
}  // namespace catalog
namespace execution {
namespace ast {
class Decl;
class Expr;
class FieldDecl;
class Stmt;
}  // namespace ast
namespace compiler {
class CodeGen;
class FunctionBuilder;
}  // namespace compiler
namespace util {
template <typename T>
class RegionVector;
}  // namespace util
}  // namespace execution
}  // namespace terrier

namespace terrier::execution::compiler {

/**
 * Update Translator
 */
class UpdateTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  UpdateTranslator(const terrier::planner::UpdatePlanNode *op, CodeGen *codegen);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override{};

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override{};

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Updates don't output anything"); };
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override;

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // Declare the updater
  void DeclareUpdater(FunctionBuilder *builder);
  // Free the updater
  void GenUpdaterFree(FunctionBuilder *builder);
  // Set the oids variable
  void SetOids(FunctionBuilder *builder);
  // Declare the update PR
  void DeclareUpdatePR(FunctionBuilder *builder);
  // Get the pr to update
  void GetUpdatePR(FunctionBuilder *builder);
  // Fill the update PR from the child's output
  void FillPRFromChild(FunctionBuilder *builder);
  // Update on table.
  void GenTableUpdate(FunctionBuilder *builder);
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder);
  // Insert into index.
  void GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);
  // Delete from table.
  void GenTableDelete(FunctionBuilder *builder);
  // Delete from index.
  void GenIndexDelete(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);

  // Get all columns oids.
  static std::vector<catalog::col_oid_t> CollectOids(const planner::UpdatePlanNode *node) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &clause : node->GetSetClauses()) {
      oids.emplace_back(clause.first);
    }
    return oids;
  }

 private:
  const planner::UpdatePlanNode *op_;
  ast::Identifier updater_;
  ast::Identifier update_pr_;
  ast::Identifier col_oids_;

  const catalog::Schema &table_schema_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
