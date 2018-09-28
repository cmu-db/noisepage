//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_translator.h
//
// Identification: src/include/execution/expression/function_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/expression/expression_translator.h"
#include "type/type.h"

namespace terrier::execution {

namespace expression {
class FunctionExpression;
}  // namespace expression



/// A translator for function expressions.
class FunctionTranslator : public ExpressionTranslator {
 public:
  FunctionTranslator(const expression::FunctionExpression &func_expr,
                     CompilationContext &context);

  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};


}  // namespace terrier::execution
