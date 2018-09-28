//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conjunction_translator.cpp
//
// Identification: src/execution/expression/conjunction_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/conjunction_translator.h"

#include "expression/conjunction_expression.h"

namespace terrier::execution {

// Constructor
ConjunctionTranslator::ConjunctionTranslator(const expression::ConjunctionExpression &conjunction,
                                             CompilationContext &context)
    : ExpressionTranslator(conjunction, context) {
  PELOTON_ASSERT(conjunction.GetChildrenSize() == 2);
}

// Produce the value that is the result of codegening the expression
Value ConjunctionTranslator::DeriveValue(CodeGen &codegen, RowBatch::Row &row) const {
  const auto &conjunction = GetExpressionAs<expression::ConjunctionExpression>();
  Value left = row.DeriveValue(codegen, *conjunction.GetChild(0));
  Value right = row.DeriveValue(codegen, *conjunction.GetChild(1));

  switch (conjunction.GetExpressionType()) {
    case ExpressionType::CONJUNCTION_AND:
      return left.LogicalAnd(codegen, right);
    case ExpressionType::CONJUNCTION_OR:
      return left.LogicalOr(codegen, right);
    default:
      throw Exception{"Received a non-conjunction expression type: " +
                      ExpressionTypeToString(conjunction.GetExpressionType())};
  }
}

}  // namespace terrier::execution