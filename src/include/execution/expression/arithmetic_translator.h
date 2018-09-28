//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// arithmetic_translator.h
//
// Identification: src/include/execution/expression/arithmetic_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/expression/expression_translator.h"

namespace terrier::execution {

namespace expression {
class OperatorExpression;
}  // namespace expression



//===----------------------------------------------------------------------===//
// A translator of arithmetic expressions.
//===----------------------------------------------------------------------===//
class ArithmeticTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ArithmeticTranslator(const expression::OperatorExpression &arithmetic,
                       CompilationContext &context);

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};


}  // namespace terrier::execution