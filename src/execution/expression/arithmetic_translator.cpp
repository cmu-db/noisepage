//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// arithmetic_translator.cpp
//
// Identification: src/execution/expression/arithmetic_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/arithmetic_translator.h"

#include "expression/operator_expression.h"

namespace terrier::execution {

// Constructor
ArithmeticTranslator::ArithmeticTranslator(const expression::OperatorExpression &arithmetic,
                                           CompilationContext &context)
    : ExpressionTranslator(arithmetic, context) {
  PELOTON_ASSERT(arithmetic.GetChildrenSize() == 2);
}

// Produce the value that is the result of codegening the expression
Value ArithmeticTranslator::DeriveValue(CodeGen &codegen, RowBatch::Row &row) const {
  const auto &arithmetic = GetExpressionAs<expression::OperatorExpression>();
  Value left = row.DeriveValue(codegen, *arithmetic.GetChild(0));
  Value right = row.DeriveValue(codegen, *arithmetic.GetChild(1));

  switch (arithmetic.GetExpressionType()) {
    case ExpressionType::OPERATOR_PLUS:
      return left.Add(codegen, right);
    case ExpressionType::OPERATOR_MINUS:
      return left.Sub(codegen, right);
    case ExpressionType::OPERATOR_MULTIPLY:
      return left.Mul(codegen, right);
    case ExpressionType::OPERATOR_DIVIDE:
      return left.Div(codegen, right);
    case ExpressionType::OPERATOR_MOD:
      return left.Mod(codegen, right);
    default: {
      throw Exception("Arithmetic expression has invalid type for translation: " +
                      ExpressionTypeToString(arithmetic.GetExpressionType()));
    }
  }
}

}  // namespace terrier::execution