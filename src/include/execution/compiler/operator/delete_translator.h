#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/delete_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Delete Translator
 */
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const terrier::planner::DeletePlanNode *op, CodeGen *codegen);

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

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Deletes don't output anything"); };

  const planner::AbstractPlanNode *Op() override { return op_; }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the updater
  void DeclareDeleter(FunctionBuilder *builder);
  void GenDeleterFree(FunctionBuilder *builder);
  // Set the oids variable
  void SetOids(FunctionBuilder *builder);
  // Delete from table.
  void GenTableDelete(FunctionBuilder *builder);
  // Delete from index.
  void GenIndexDelete(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);
  // Get all columns oids.
  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  const planner::DeletePlanNode *op_;
  static constexpr const char *deleter_name_ = "deleter";
  static constexpr const char *col_oids_name_ = "col_oids";
  ast::Identifier deleter_;
  ast::Identifier col_oids_;

  // TODO(Amadou): If tpl supports null arrays, leave this empty. Otherwise, put a dummy value of 1 inside.
  std::vector<catalog::col_oid_t> oids_;
};

}  // namespace terrier::execution::compiler
