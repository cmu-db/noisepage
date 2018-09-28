//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.h
//
// Identification: src/include/execution/expression/negation_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/expression/expression_translator.h"

namespace terrier::execution {

namespace expression {
class OperatorUnaryMinusExpression;
}  // namespace expression

class NegationTranslator : public ExpressionTranslator {
 public:
  // Constructor
  NegationTranslator(const expression::OperatorUnaryMinusExpression &unary_minus_expression, CompilationContext &ctx);

  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
};

}  // namespace terrier::execution