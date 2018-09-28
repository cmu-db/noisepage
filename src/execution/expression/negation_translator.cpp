//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.cpp
//
// Identification: src/execution/expression/negation_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/negation_translator.h"

#include "execution/type/type_system.h"
#include "expression/operator_expression.h"

namespace terrier::execution {


// Constructor
NegationTranslator::NegationTranslator(
    const expression::OperatorUnaryMinusExpression &unary_minus_expression,
    CompilationContext &ctx)
    : ExpressionTranslator(unary_minus_expression, ctx) {
  PELOTON_ASSERT(unary_minus_expression.GetChildrenSize() == 1);
}

Value NegationTranslator::DeriveValue(CodeGen &codegen,
                                      RowBatch::Row &row) const {
  const auto &negation_expr =
      GetExpressionAs<expression::OperatorUnaryMinusExpression>();
  Value child_value = row.DeriveValue(codegen, *negation_expr.GetChild(0));
  return child_value.CallUnaryOp(codegen, OperatorId::Negation);
}


}  // namespace terrier::execution