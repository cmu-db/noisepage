#include "execution/compiler/operator/limit_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
void LimitTranslator::Produce(FunctionBuilder *builder) {
  builder->Append(codegen_->DeclareVariable(num_tuples_, nullptr, codegen_->IntLiteral(0)));
  child_translator_->Produce(builder);
}

void LimitTranslator::Consume(FunctionBuilder *builder) {
  // Check for offset and limit.
  // if (num_tuples_ >= offset && num_tuples_ < limit + offset)
  auto offset_comp = codegen_->Compare(parsing::Token::Type::GREATER_EQUAL, codegen_->MakeExpr(num_tuples_),
                                       codegen_->IntLiteral(op_->GetOffset()));
  auto limit_comp = codegen_->Compare(parsing::Token::Type::LESS, codegen_->MakeExpr(num_tuples_),
                                      codegen_->IntLiteral(op_->GetLimit() + op_->GetOffset()));
  auto if_cond = codegen_->BinaryOp(parsing::Token::Type::AND, offset_comp, limit_comp);
  builder->StartIfStmt(if_cond);
  parent_translator_->Consume(builder);
  // Close if stmt
  builder->FinishBlockStmt();
  // Increment num tuples
  auto lhs = codegen_->MakeExpr(num_tuples_);
  auto rhs = codegen_->BinaryOp(parsing::Token::Type::PLUS, codegen_->MakeExpr(num_tuples_), codegen_->IntLiteral(1));
  builder->Append(codegen_->Assign(lhs, rhs));
}
}  // namespace terrier::execution::compiler
