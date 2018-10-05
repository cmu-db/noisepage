//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_translator.cpp
//
// Identification: src/execution/expression/comparison_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expression/comparison_translator.h"
#include "execution/type/type_system.h"

#include "expression/comparison_expression.h"

namespace terrier::execution {

// Constructor
ComparisonTranslator::ComparisonTranslator(const expression::ComparisonExpression &comparison,
                                           CompilationContext &context)
    : ExpressionTranslator(comparison, context) {
  TERRIER_ASSERT(comparison.GetChildrenSize() == 2, "Must have exactly two children.");
}

// Produce the result of performing the comparison of left and right values
Value ComparisonTranslator::DeriveValue(CodeGen &codegen, RowBatch::Row &row) const {
  const auto &comparison = GetExpressionAs<expression::ComparisonExpression>();

  Value left = row.DeriveValue(codegen, *comparison.GetChild(0));
  Value right = row.DeriveValue(codegen, *comparison.GetChild(1));

  switch (comparison.GetExpressionType()) {
    case ExpressionType::COMPARE_EQUAL:
      return left.CompareEq(codegen, right);
    case ExpressionType::COMPARE_NOTEQUAL:
      return left.CompareNe(codegen, right);
    case ExpressionType::COMPARE_LESSTHAN:
      return left.CompareLt(codegen, right);
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return left.CompareLte(codegen, right);
    case ExpressionType::COMPARE_GREATERTHAN:
      return left.CompareGt(codegen, right);
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return left.CompareGte(codegen, right);
    case ExpressionType::COMPARE_LIKE: {
      type::TypeSystem::InvocationContext ctx{.on_error = OnError::Exception,
                                              .executor_context = GetExecutionContextPtr()};

      type::Type left_type = left.GetType(), right_type = right.GetType();
      auto *binary_op =
          type::TypeSystem::GetBinaryOperator(OperatorId::Like, left_type, left_type, right_type, right_type);
      TERRIER_ASSERT(binary_op != nullptr, "Binary operator cannot be null.");
      return binary_op->Eval(codegen, left.CastTo(codegen, left_type), right.CastTo(codegen, right_type), ctx);
    }
    default: {
      throw Exception{"Invalid expression type for translation " +
                      ExpressionTypeToString(comparison.GetExpressionType())};
    }
  }
}

}  // namespace terrier::execution
