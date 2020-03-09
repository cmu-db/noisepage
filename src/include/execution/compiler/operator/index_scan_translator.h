#pragma once
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/index_schema.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/index_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Index scan translator.
 */
class IndexScanTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  IndexScanTranslator(const terrier::planner::IndexScanPlanNode *op, CodeGen *codegen);

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
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  // This is a materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = false;
    return true;
  }

  // Return the projected row and its type
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    return {&table_pr_, &pr_type_};
  }

  ast::Expr *GetOutput(uint32_t attr_idx) override;
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override;

  ast::Expr *GetSlot() override { return codegen_->PointerTo(slot_); }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // Declare the index iterator
  void DeclareIterator(FunctionBuilder *builder);
  // Set the column oids to scan
  void SetOids(FunctionBuilder *builder);
  // Fill the key with table data
  void FillKey(FunctionBuilder *builder, ast::Identifier pr,
               const std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> &index_exprs);
  // Generate the index iteration loop
  void GenForLoop(FunctionBuilder *builder);
  // Generate the join predicate's if statement
  void GenPredicate(FunctionBuilder *builder);
  // Free the iterator
  void FreeIterator(FunctionBuilder *builder);
  // Get Index PR
  void DeclareIndexPR(FunctionBuilder *builder);
  // Get Table PR
  void DeclareTablePR(FunctionBuilder *builder);
  // Get Slot
  void DeclareSlot(FunctionBuilder *builder);

 private:
  const planner::IndexScanPlanNode *op_;
  std::vector<catalog::col_oid_t> input_oids_;
  const catalog::Schema &table_schema_;
  storage::ProjectionMap table_pm_;
  const catalog::IndexSchema &index_schema_;
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm_;
  // Structs and local variables
  ast::Identifier index_iter_;
  ast::Identifier col_oids_;
  ast::Identifier index_pr_;
  ast::Identifier lo_index_pr_;
  ast::Identifier hi_index_pr_;
  ast::Identifier table_pr_;
  ast::Identifier pr_type_;
  ast::Identifier slot_;
};
}  // namespace terrier::execution::compiler
