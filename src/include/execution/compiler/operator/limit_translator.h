#pragma once

#include <stdint.h>
#include <memory>
#include <utility>
#include <vector>

#include "brain/brain_defs.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/util/execution_common.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/output_schema.h"
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
 * Limit Translator
 */
class LimitTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  LimitTranslator(const terrier::planner::LimitPlanNode *op, CodeGen *codegen)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::LIMIT),
        op_(op),
        num_tuples_(codegen->NewIdentifier("num_tuples")) {}

  // Pass through
  void Produce(FunctionBuilder *builder) override;

  // Pass through
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }

  // Pass through
  void Consume(FunctionBuilder *builder) override;

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

  ast::Expr *GetOutput(uint32_t attr_idx) override {
    auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
    auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
    return translator->DeriveExpr(this);
  }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return child_translator_->GetOutput(attr_idx);
  }

  // Is always vectorizable.
  bool IsVectorizable() override { return true; }

  // Should not be called here
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override {
    UNREACHABLE("Projection nodes should not use column value expressions");
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::LimitPlanNode *op_;
  ast::Identifier num_tuples_;
};

}  // namespace terrier::execution::compiler
