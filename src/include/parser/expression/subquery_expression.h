#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/select_statement.h"
#include "type/type_id.h"

namespace noisepage::parser {
/**
 * SubqueryExpression represents an expression which contains a select statement ("sub-select").
 */
class SubqueryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new SubqueryExpression with the given sub-select from the parser.
   * @param subselect the sub-select
   */
  explicit SubqueryExpression(std::unique_ptr<parser::SelectStatement> subselect)
      : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID, {}), subselect_(std::move(subselect)) {}

  /** Default constructor for JSON deserialization. */
  SubqueryExpression() = default;

  /**
   * Copies this SubqueryExpression
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
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "SubqueryExpression should have 0 children");
    return Copy();
  }

  /** @return managed pointer to the sub-select */
  common::ManagedPointer<parser::SelectStatement> GetSubselect() { return common::ManagedPointer(subselect_); }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /**
   * TODO(WAN): document the depths, ask Ling
   * @return Derived depth of the expression
   */
  int DeriveDepth() override;

  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const SubqueryExpression &>(rhs);
    return *subselect_ == *(other.subselect_);
  }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /** Sub-select statement. */
  std::unique_ptr<SelectStatement> subselect_;
};

DEFINE_JSON_HEADER_DECLARATIONS(SubqueryExpression);

}  // namespace noisepage::parser
