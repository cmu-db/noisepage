#pragma once

#include <unordered_map>
#include <utility>
#include "execution/ast/ast.h"
#include "execution/compiler/row_batch.h"
#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {

// clang-format off
#define COMPARISON_OP(type)                                               \
  ((type) <= terrier::parser::ExpressionType::COMPARE_IS_DISTINCT_FROM && \
    (type) >= terrier::parser::ExpressionType::COMPARE_EQUAL)

#define ARITHMETIC_OP(type) \
  ((type) <= terrier::parser::ExpressionType::OPERATOR_MOD && (type) >= terrier::parser::ExpressionType::OPERATOR_PLUS)

#define UNARY_OP(type)                                                \
  ((type) == terrier::parser::ExpressionType::OPERATOR_UNARY_MINUS || \
    ((type) >= terrier::parser::ExpressionType::OPERATOR_CAST &&       \
      (type) <= terrier::parser::ExpressionType::OPERATOR_EXISTS))

#define CONJUNCTION_OP(type)                                    \
  ((type) <= terrier::parser::ExpressionType::CONJUNCTION_OR && \
    (type) >= terrier::parser::ExpressionType::CONJUNCTION_AND)

#define CONSTANT_VAL(type) ((type) <= terrier::parser::ExpressionType::VALUE_CONSTANT)

#define TUPLE_VAL(type) ((type) <= terrier::parser::ExpressionType::VALUE_TUPLE)

#define NULL_OP(type)                                             \
  ((type) >= terrier::parser::ExpressionType::OPERATOR_IS_NULL && \
    (type) <= terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL)
// clang-format on

class CompilationContext;

class ExpressionTranslator {
 public:
  ExpressionTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context)
      : context_(context), expression_(*expression) {}

  virtual ~ExpressionTranslator() = default;

  virtual ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) = 0;

  template <typename T>
  const T &GetExpressionAs() const {
    return static_cast<const T &>(expression_);
  }

 protected:
  CompilationContext *context_;

 private:
  const terrier::parser::AbstractExpression &expression_;
};
};  // namespace tpl::compiler
