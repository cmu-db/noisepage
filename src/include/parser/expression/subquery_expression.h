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
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<common::ManagedPointer<AbstractExpression>> select_columns;
    for (const auto &col : subselect_->GetSelectColumns()) {
      select_columns.emplace_back(common::ManagedPointer(col));
    }

    auto group_by = subselect_->GetSelectGroupBy() == nullptr ? nullptr : subselect_->GetSelectGroupBy()->Copy();
    auto order_by = subselect_->GetSelectOrderBy() == nullptr ? nullptr : subselect_->GetSelectOrderBy()->Copy();
    auto limit = subselect_->GetSelectLimit() == nullptr ? nullptr : subselect_->GetSelectLimit()->Copy();
    std::vector<std::unique_ptr<TableRef>> with;
    for (auto &ref : subselect_->GetSelectWith()) {
      with.push_back(ref->Copy());
    }

    auto parser_select = std::make_unique<SelectStatement>(
        std::move(select_columns), subselect_->IsSelectDistinct(), subselect_->GetSelectTable()->Copy(),
        subselect_->GetSelectCondition(), std::move(group_by), std::move(order_by), std::move(limit), std::move(with));
    auto expr = std::make_unique<SubqueryExpression>(std::move(parser_select));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

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
