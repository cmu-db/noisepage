#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "binder/binder_sherpa.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * ConjunctionExpression represents logical conjunctions like ANDs and ORs.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right  TODO(WAN): wtf? tpcc_plan_delivery_test
   */
  ConjunctionExpression(const ExpressionType cmp_type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /** Default constructor for deserialization. */
  ConjunctionExpression() = default;

  /**
   * Copies ConjunctionExpression
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
    auto expr = std::make_unique<ConjunctionExpression>(GetExpressionType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v,
              common::ManagedPointer<binder::BinderSherpa> sherpa) override {
    sherpa->CheckDesiredType(common::ManagedPointer(this).CastManagedPointerTo<AbstractExpression>());

    for (const auto child : GetChildren()) {
      sherpa->SetDesiredType(child, type::TypeId::BOOLEAN);
    }

    v->Visit(common::ManagedPointer(this), sherpa);
  }
};

DEFINE_JSON_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
