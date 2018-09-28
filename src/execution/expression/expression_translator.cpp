//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.cpp
//
// Identification: src/execution/expression/expression_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/expression_translator.h"

#include "execution/compilation_context.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace terrier::execution {

ExpressionTranslator::ExpressionTranslator(const expression::AbstractExpression &expression, CompilationContext &ctx)
    : context_(ctx), expression_(expression) {
  if (expression::ExpressionUtil::IsAggregateExpression(expression.GetExpressionType())) return;
  for (uint32_t i = 0; i < expression_.GetChildrenSize(); i++) {
    ctx.Prepare(*expression_.GetChild(i));
  }
}

llvm::Value *ExpressionTranslator::GetExecutorContextPtr() const {
  return context_.GetExecutionConsumer().GetExecutorContextPtr(context_);
}

}  // namespace terrier::execution
