#pragma once

#include <memory>
#include <string>
#include <utility>
#include "execution/ast/context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/exec/output.h"

namespace terrier::execution::compiler {
/**
 * Consumer that generates code for outputting to upper layers.
 */
class OutputTranslator : public OperatorTranslator {
 public:
  /**
   * Construction
   * @param schema final schema returned to the upper layers
   */
  explicit OutputTranslator(CodeGen *codegen);

  void Produce(FunctionBuilder *builder) override;

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
  // TODO(Amadou): throw an exception?
  ast::Expr *GetOutput(uint32_t attr_idx) override { return nullptr; }

  // Should never be called for the same reasons.
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return nullptr;
  }

 private:
  // Return the output field at the given index
  ast::Expr *GetField(uint32_t attr_idx);

  // Generates: var out = @ptrCast(*Output, @outputAlloc(execCtx))
  void DeclareOutputVariable(FunctionBuilder *builder);

  // Fills the output slot
  void FillOutput(FunctionBuilder *builder);

  // Advance the output buffer
  void AdvanceOutput(FunctionBuilder *builder);

  // Register @outputFinalize(execCtx) at the end of the pipeline
  void FinalizeOutput(FunctionBuilder *builder);

  // Number of output fields;
  uint32_t num_output_fields_{0};
  // Structs and local variables
  static constexpr const char *output_struct_name_ = "Output";
  static constexpr const char *output_var_name_ = "out";
  static constexpr const char *output_field_prefix_ = "col";
  ast::Identifier output_struct_;
  ast::Identifier output_var_;
};

}  // namespace terrier::execution::compiler
