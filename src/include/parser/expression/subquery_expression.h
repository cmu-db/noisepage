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

  std::unique_ptr<AbstractExpression> Copy() const override {
    /*
    std::vector<std::unique_ptr<AbstractExpression>> select_columns;
    for (const auto &col : subselect_->GetSelectColumns()) {
      select_columns.emplace_back(col->Copy());
    }
    // TODO(WAN) sigh..
    auto parser_select = std::make_unique<SelectStatement>(
        std::move(select_columns), subselect_->IsSelectDistinct(),
        subselect_->GetSelectTable(), subselect_->GetSelectCondition()->Copy(),
        subselect_->GetSelectGroupBy(), subselect_->GetSelectOrderBy(),
        subselect_->GetSelectLimit()
        );
    return std::make_unique<SubqueryExpression>(std::move(parser_select));
     */
    return std::make_unique<SubqueryExpression>(nullptr);
  }

  /** @return managed pointer to the sub-select */
  common::ManagedPointer<parser::SelectStatement> GetSubselect() { return common::ManagedPointer(subselect_); }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

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
      auto where_depth = where->DeriveDepth();
      if (where_depth >= 0 && where_depth < current_depth) this->SetDepth(where_depth);
    }
    return this->GetDepth();
  }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    for (auto &select_elem : subselect_->GetSelectColumns())
      hash = common::HashUtil::CombineHashes(hash, select_elem->Hash());

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
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    subselect_ = std::make_unique<parser::SelectStatement>();
    subselect_->FromJson(j.at("subselect"));
  }

 private:
  /** Sub-select statement. */
  std::unique_ptr<SelectStatement> subselect_;
};

DEFINE_JSON_DECLARATIONS(SubqueryExpression);

}  // namespace terrier::parser
