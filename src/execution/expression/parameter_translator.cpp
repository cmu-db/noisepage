//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.cpp
//
// Identification: src/execution/expression/parameter_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/parameter_translator.h"
#include "execution/compilation_context.h"
#include "expression/parameter_value_expression.h"

namespace terrier::execution {

// Constructor
ParameterTranslator::ParameterTranslator(const expression::ParameterValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

// Return an LLVM value for the constant: run-time value
Value ParameterTranslator::DeriveValue(UNUSED_ATTRIBUTE CodeGen &codegen,
                                                UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  const auto &expr = GetExpressionAs<expression::ParameterValueExpression>();
  const auto &parameter_cache = context_.GetParameterCache();
  return parameter_cache.GetValue(&expr);
}

}  // namespace terrier::execution
