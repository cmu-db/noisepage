//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/execution/expression/constant_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/compilation_context.h"
#include "execution/expression/constant_translator.h"
#include "expression/constant_value_expression.h"

namespace terrier::execution {


// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

// Return an LLVM value for our constant: values passed over at run time
codegen::Value ConstantTranslator::DeriveValue(
    UNUSED_ATTRIBUTE CodeGen &codegen,
    UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  const auto &expr = GetExpressionAs<expression::ConstantValueExpression>();
  const auto &parameter_cache = context_.GetParameterCache();
  return parameter_cache.GetValue(&expr);
}


}  // namespace terrier::execution
