#pragma once

#include <unordered_map>
#include <utility>
#include "execution/ast/ast.h"
#include "parser/expression/abstract_expression.h"
#include "execution/compiler/codegen.h"

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
 * These methods have been seperated from the OperatorTranslator to allow
 * arbitrary components of the system to evaluate expressions.
 */
class ExpressionEvaluator {
 public:
  /**
 * @param child_idx index of the child (0 or 1)
 * @param attr_idx index of the child's output
 * @param type type of the attribute
 * @return the child's output at the given index
 */
  virtual ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) = 0;

  /**
   * Return a table column value.
   * @param col_oid oid of the column
   * @return an expression representing the value
   */
  virtual ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) {
    UNREACHABLE("This operator does not interact with tables");
  }

};

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
  virtual ast::Expr *DeriveExpr(ExpressionEvaluator *evaluator) = 0;

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
