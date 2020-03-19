#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/select_statement.h"
#include "type/type_id.h"

namespace terrier::parser {
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

    auto parser_select = std::make_unique<SelectStatement>(
        std::move(select_columns), subselect_->IsSelectDistinct(), subselect_->GetSelectTable()->Copy(),
        subselect_->GetSelectCondition(), std::move(group_by), std::move(order_by), std::move(limit));
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
    TERRIER_ASSERT(children.empty(), "SubqueryExpression should have 0 children");
    return Copy();
  }

  /** @return managed pointer to the sub-select */
  common::ManagedPointer<parser::SelectStatement> GetSubselect() { return common::ManagedPointer(subselect_); }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v,
              common::ManagedPointer<binder::BinderSherpa> sherpa) override {
    v->Visit(common::ManagedPointer(this), sherpa);
  }

  /**
   * TODO(WAN): document the depths, ask Ling
   * @return Derived depth of the expression
   */
  int DeriveDepth() override {
    int current_depth = this->GetDepth();
    for (auto &select_elem : subselect_->GetSelectColumns()) {
      int select_depth = select_elem->DeriveDepth();
      if (select_depth >= 0 && (current_depth == -1 || select_depth < current_depth)) {
        this->SetDepth(select_depth);
        current_depth = select_depth;
      }
    }
    auto where = subselect_->GetSelectCondition();
    if (where != nullptr) {
      auto where_depth = const_cast<parser::AbstractExpression *>(where.Get())->DeriveDepth();
      if (where_depth >= 0 && where_depth < current_depth) this->SetDepth(where_depth);
    }
    return this->GetDepth();
  }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    for (auto select_elem : subselect_->GetSelectColumns()) {
      hash = common::HashUtil::CombineHashes(hash, select_elem->Hash());
    }

    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(subselect_->IsSelectDistinct()));
    if (subselect_->GetSelectCondition() != nullptr)
      hash = common::HashUtil::CombineHashes(hash, subselect_->GetSelectCondition()->Hash());
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const SubqueryExpression &>(rhs);
    return *subselect_ == *(other.subselect_);
  }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["subselect"] = subselect_->ToJson();
    return j;
  }

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    subselect_ = std::make_unique<parser::SelectStatement>();
    auto e2 = subselect_->FromJson(j.at("subselect"));
    exprs.insert(exprs.end(), std::make_move_iterator(e2.begin()), std::make_move_iterator(e2.end()));
    return exprs;
  }

 private:
  /** Sub-select statement. */
  std::unique_ptr<SelectStatement> subselect_;
};

DEFINE_JSON_DECLARATIONS(SubqueryExpression);

}  // namespace terrier::parser
