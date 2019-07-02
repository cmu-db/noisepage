#include "execution/compiler/operator/output_translator.h"

namespace tpl::compiler {
OutputTranslator::OutputTranslator(tpl::compiler::CodeGen *codegen)
: OperatorTranslator(nullptr, codegen)
, output_struct_(codegen->NewIdentifier(output_struct_name_))
, output_var_(codegen->NewIdentifier(output_var_name_))
{}

void OutputTranslator::InitializeStructs(tpl::util::RegionVector<tpl::ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl*> fields(codegen_->Region());
  GetChildOutputFields(&fields, output_field_prefix_);
  num_output_fields_ = static_cast<uint32_t>(fields.size());
  decls->emplace_back(codegen_->MakeStruct(output_struct_, std::move(fields)));
}

ast::Expr* OutputTranslator::GetField(uint32_t attr_idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(output_field_prefix_ + std::to_string(attr_idx));
  return codegen_->MemberExpr(output_var_, member);
}

void OutputTranslator::Produce(tpl::compiler::FunctionBuilder *builder) {
  // First declare the output variable
  DeclareOutputVariable(builder);
  // Fill in the output
  FillOutput(builder);
  // Advance the output buffer with @outputAdvance()
  AdvanceOutput(builder);
  // Register the @outputFinalize() call at the end of the pipepeline.
  FinalizeOutput(builder);
}

void OutputTranslator::DeclareOutputVariable(tpl::compiler::FunctionBuilder *builder) {
  // First generate the call @outputAlloc(execCtx)
  ast::Expr* alloc_call = codegen_->OutputAlloc();
  // The make the @ptrCast call
  ast::Expr* cast_call = codegen_->PtrCast(output_struct_, alloc_call);
  // Finally, declare the variable
  builder->Append(codegen_->DeclareVariable(output_var_, nullptr, cast_call));
}

void OutputTranslator::FillOutput(tpl::compiler::FunctionBuilder *builder) {
  // For each column in the output, set out.col_i = col_i
  for (uint32_t attr_idx = 0; attr_idx < num_output_fields_; attr_idx++) {
    ast::Expr* lhs = GetField(attr_idx);
    ast::Expr* rhs = prev_translator_->GetOutput(attr_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void OutputTranslator::AdvanceOutput(tpl::compiler::FunctionBuilder *builder) {
  // Call @outputAdvance(execCtx)
  ast::Expr* advance_call = codegen_->OutputAdvance();
  builder->Append(codegen_->MakeStmt(advance_call));
}

void OutputTranslator::FinalizeOutput(tpl::compiler::FunctionBuilder *builder) {
  ast::Expr* finalize_call = codegen_->OutputFinalize();
  builder->RegisterFinalStmt(codegen_->MakeStmt(finalize_call));
}

}