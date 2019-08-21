#pragma once
#include <planner/plannodes/index_join_plan_node.h>
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::execution::compiler {

/**
 * Index Nested Loop join translator.
 */
class IndexJoinTranslator : public OperatorTranslator {
 public:
  IndexJoinTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  void Produce(FunctionBuilder *builder) override;

  void Consume(FunctionBuilder *builder) override;

  // Pass Through
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override;

 private:
  // Declare the index iterator
  void DeclareIterator(FunctionBuilder *builder);
  // Set the column oids to scan
  void SetOids(FunctionBuilder *builder);
  // Fill the key with table data
  void FillKey(FunctionBuilder *builder);
  // Generate the index iteration loop
  void GenForLoop(FunctionBuilder *builder);
  // Generate the join predicate's if statement
  void GenPredicate(FunctionBuilder *builder);
  // Free the iterator
  void FreeIterator(FunctionBuilder *builder);

 private:
  const planner::IndexJoinPlanNode *index_join_;
  std::vector<catalog::col_oid_t> input_oids_;
  const catalog::Schema &table_schema_;
  storage::ProjectionMap table_pm_;
  const catalog::IndexSchema &index_schema_;
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm_;
  // Structs and local variables
  static constexpr const char *iter_name_ = "index_iter";
  static constexpr const char *col_oids_name_ = "col_oids_";
  ast::Identifier index_iter_;
  ast::Identifier col_oids_;
};
}  // namespace terrier::execution::compiler