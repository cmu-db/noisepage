#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * TypeCastExpression represents cast expressions of the form CAST(expr) or expr::TYPE.
 *
 * For constants, TypeCastExpression should not exist beyond the optimizer, and the child should be used instead.
 * The role of a TypeCastExpression is to annotate the type of expr from above in the BinderSherpa.
 * For example, Postgres does not allow CAST('1+1' AS BIGINT), throwing error 22P02.
 *
 * However, for non-constant expressions such as the following trace,
 *   CREATE TABLE foo (a INTEGER);
 *   INSERT INTO foo VALUES (1);
 *   SELECT a::DECIMAL FROM foo;
 * Then the casting must be done at execution time. As of 2020/11/14, non-constant expressions are not supported.
 */
class TypeCastExpression : public AbstractExpression {
  // TODO(Ling):  Do we need a separate class for operator_cast? We can put it in operatorExpression
  // Wan: can you elaborate? How do you envision this being used?
 public:
  /** Instantiates a new type cast expression. */
  TypeCastExpression(type::TypeId type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)) {}

  /** Default constructor for JSON deserialization. */
  TypeCastExpression() = default;

  /**
   * Copies this TypeCastExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;
  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;
};

DEFINE_JSON_HEADER_DECLARATIONS(TypeCastExpression);

}  // namespace noisepage::parser
