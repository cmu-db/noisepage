#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * ComparisonExpression represents comparisons between multiple expressions like < and >.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param children vector containing exactly two children, left then right
   */
  ComparisonExpression(const ExpressionType cmp_type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /** Default constructor for deserialization. */
  ComparisonExpression() = default;

  /**
   * Copies the ComparisonExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    auto expr = std::make_unique<ComparisonExpression>(GetExpressionType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override {
    v->Visit(this, parse_result);
    // TODO(WAN): This is basically a hack to rebind NULLs with the right type and I'm not sure it should stay.
    // Generated NULL expressions are owned by the children vector, so the generated expressions do not need to be
    // added to the parse result. The old expression that is kicked out will die a unique_ptr death.
    auto left = GetChild(0);
    auto right = GetChild(1);
    auto left_type = left->GetReturnValueType();
    auto right_type = right->GetReturnValueType();

    auto is_left_subquery = left->GetExpressionType() == ExpressionType::ROW_SUBQUERY;
    auto is_right_subquery = right->GetExpressionType() == ExpressionType::ROW_SUBQUERY;

    // TODO(WAN): I don't know how to handle this case and casting to NULL is not it.
    if (is_left_subquery || is_right_subquery) {
      return;
    }

    if (left_type == type::TypeId::INVALID && right_type != type::TypeId::INVALID) {
      auto new_left_tv = type::TransientValueFactory::GetNull(right_type);
      auto new_left = std::make_unique<ConstantValueExpression>(std::move(new_left_tv));
      children_[0] = std::move(new_left);
    }

    if (left_type != type::TypeId::INVALID && right_type == type::TypeId::INVALID) {
      auto new_right_tv = type::TransientValueFactory::GetNull(left_type);
      auto new_right = std::make_unique<ConstantValueExpression>(std::move(new_right_tv));
      children_[1] = std::move(new_right);
    }
  }
};

DEFINE_JSON_DECLARATIONS(ComparisonExpression);

}  // namespace terrier::parser
