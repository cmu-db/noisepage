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

namespace peloton {

namespace expression {
class OperatorExpression;
}  // namespace expression

namespace codegen {

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

}  // namespace codegen
}  // namespace peloton