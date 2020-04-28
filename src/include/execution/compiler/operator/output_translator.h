#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "execution/ast/context.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/exec/output.h"
#include "execution/util/execution_common.h"
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
namespace planner {
class AbstractPlanNode;
}  // namespace planner
}  // namespace terrier

namespace terrier::execution::compiler {
/**
 * Consumer that generates code for outputting to upper layers.
 */
class OutputTranslator : public OperatorTranslator {
 public:
  /**
   * Construction
   * @param codegen The code generator
   */
  explicit OutputTranslator(CodeGen *codegen);

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override {}
  void Consume(FunctionBuilder *builder) override;

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Create the output struct
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  // Should never called since this the last layer.
  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Should not be called on this translator"); }

  // Should never be called for the same reasons.
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("Should not be called on this translator");
  }

  const planner::AbstractPlanNode *Op() override { UNREACHABLE("Should not be called on this translator"); }

 private:
  // Return the output field at the given index
  ast::Expr *GetField(uint32_t attr_idx);

  // Generates: var out = @ptrCast(*Output, @outputAlloc(execCtx))
  void DeclareOutputVariable(FunctionBuilder *builder);

  // Fills the output slot
  void FillOutput(FunctionBuilder *builder);

  // Call @outputFinalize(execCtx) at the end of the pipeline
  void FinalizeOutput(FunctionBuilder *builder);

  // Number of output fields;
  uint32_t num_output_fields_{0};
  // Structs and local variables
  ast::Identifier output_struct_;
  ast::Identifier output_var_;
};

}  // namespace terrier::execution::compiler
