#pragma once

#include "../../../planner/plannodes/update_plan_node.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/storage/pr_filler.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Update Translator
 */
class UpdateTranslator : public OperatorTranslator {
 public:
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

  // Produce and consume logic
  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  // This is not a materializer
  bool IsMaterializer(bool *is_ptr) override { return false; }

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Updates don't output anything"); };

  const planner::AbstractPlanNode *Op() override { return op_; }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the updater
  void DeclareUpdater(FunctionBuilder *builder);
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
  static constexpr const char *updater_name_ = "updater";
  static constexpr const char *update_pr_name_ = "update_pr";
  static constexpr const char *col_oids_name_ = "col_oids";
  ast::Identifier updater_;
  ast::Identifier update_pr_;
  ast::Identifier col_oids_;

  const catalog::Schema &table_schema_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
