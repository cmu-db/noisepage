#pragma once
#include <stdint.h>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/util/execution_common.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier {
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
 * Insert Translator
 */
class InsertTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  InsertTranslator(const terrier::planner::InsertPlanNode *op, CodeGen *codegen);

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

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Inserts don't output anything"); };
  const planner::AbstractPlanNode *Op() override { return op_; }
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the inserter
  void DeclareInserter(FunctionBuilder *builder);
  void GenInserterFree(FunctionBuilder *builder);
  // Set the oids variable
  void SetOids(FunctionBuilder *builder);
  // Declare the insert PR
  void DeclareInsertPR(FunctionBuilder *builder);
  // Get the pr to insert
  void GetInsertPR(FunctionBuilder *builder);
  // Fill the insert PR from the child's output
  void FillPRFromChild(FunctionBuilder *builder);
  // Set the table PR from raw values
  void GenSetTablePR(FunctionBuilder *builder, uint32_t idx);
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder);
  // Insert into index.
  void GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);
  // Get all columns oids.
  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  const planner::InsertPlanNode *op_;
  ast::Identifier inserter_;
  ast::Identifier insert_pr_;
  ast::Identifier col_oids_;

  const catalog::Schema &table_schema_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
