#pragma once

#include <unordered_map>
#include <utility>
#include "execution/ast/ast.h"
#include "execution/compiler/operator/operator_translator.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::execution::compiler {

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

#define CONSTANT_VAL(type) ((type) == terrier::parser::ExpressionType::VALUE_CONSTANT)

#define COLUMN_VAL(type) ((type) == terrier::parser::ExpressionType::COLUMN_VALUE)

#define DERIVED_VAL(type) ((type) == terrier::parser::ExpressionType::VALUE_TUPLE)

#define NULL_OP(type)                                             \
  ((type) >= terrier::parser::ExpressionType::OPERATOR_IS_NULL && \
   (type) <= terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL)

/**
 * Expression Translator
 */
class ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  ExpressionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
      : codegen_(codegen), expression_(expression) {}

  /**
   * Destructor
   */
  virtual ~ExpressionTranslator() = default;

  /**
   * TODO(Amadou): Passing expression again here may be redundant? Check with Tanuj.
   * @param op_state the operator state used to translate the expression
   * @return resulting TPL expression
   */
  virtual ast::Expr *DeriveExpr(OperatorTranslator *translator) = 0;

  /**
   * Convert the generic expression to the given type.
   * @tparam T type to convert to.
   * @return the converted expression.
   */
  template <typename T>
  const T *GetExpressionAs() {
    return reinterpret_cast<const T *>(expression_);
  }

 protected:
  /**
   * Code Generator
   */
  CodeGen *codegen_;

  /**
   * The expression to translate
   */
  const terrier::parser::AbstractExpression *expression_;
};
};  // namespace terrier::execution::compiler
