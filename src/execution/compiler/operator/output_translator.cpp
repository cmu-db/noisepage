#include "execution/compiler/operator/output_translator.h"

namespace terrier::execution::compiler {
OutputTranslator::OutputTranslator(execution::compiler::CodeGen *codegen)
    : OperatorTranslator(codegen),
      output_struct_(codegen->NewIdentifier(output_struct_name_)),
      output_var_(codegen->NewIdentifier(output_var_name_)) {}

void OutputTranslator::InitializeStructs(execution::util::RegionVector<execution::ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl *> fields(codegen_->Region());
  GetChildOutputFields(&fields, output_field_prefix_);
  num_output_fields_ = static_cast<uint32_t>(fields.size());
  decls->emplace_back(codegen_->MakeStruct(output_struct_, std::move(fields)));
}

ast::Expr *OutputTranslator::GetField(uint32_t attr_idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(output_field_prefix_ + std::to_string(attr_idx));
  return codegen_->MemberExpr(output_var_, member);
}

void OutputTranslator::Produce(execution::compiler::FunctionBuilder *builder) {
  // Let the child produce
  child_translator_->Produce(builder);

  // Call @outputFinalize() at the end of the pipeline.
  FinalizeOutput(builder);
}

void OutputTranslator::Consume(terrier::execution::compiler::FunctionBuilder *builder) {
  // First declare the output variable
  DeclareOutputVariable(builder);
  // Fill in the output
  FillOutput(builder);
}

void OutputTranslator::DeclareOutputVariable(execution::compiler::FunctionBuilder *builder) {
  // First generate the call @outputAlloc(execCtx)
  ast::Expr *alloc_call = codegen_->OutputAlloc();
  // The make the @ptrCast call
  ast::Expr *cast_call = codegen_->PtrCast(output_struct_, alloc_call);
  // Finally, declare the variable
  builder->Append(codegen_->DeclareVariable(output_var_, nullptr, cast_call));
}

void OutputTranslator::FillOutput(execution::compiler::FunctionBuilder *builder) {
  // For each column in the output, set out.col_i = col_i
  for (uint32_t attr_idx = 0; attr_idx < num_output_fields_; attr_idx++) {
    ast::Expr *lhs = GetField(attr_idx);
    ast::Expr *rhs = child_translator_->GetOutput(attr_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void OutputTranslator::FinalizeOutput(execution::compiler::FunctionBuilder *builder) {
  ast::Expr *finalize_call = codegen_->OutputFinalize();
  builder->Append(codegen_->MakeStmt(finalize_call));
}

}  // namespace terrier::execution::compiler