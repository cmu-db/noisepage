#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/select_statement.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a sub-select query.
 */
class SubqueryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new SubqueryExpression with the given sub-select from the parser.
   * @param subselect the sub-select
   */
  explicit SubqueryExpression(std::shared_ptr<parser::SelectStatement> subselect)
      : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID, {}), subselect_(std::move(subselect)) {}

  /**
   * Default constructor for deserialization
   */
  SubqueryExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override {
    // TODO(WAN): Previous codebase described as a hack, will we need a deep copy?
    // Tianyu: No need for deep copy if your objects are always immutable! (why even copy at all, but that's beyond me)
    return std::make_shared<SubqueryExpression>(*this);
  }

  /**
   * @return shared pointer to stored sub-select
   */
  std::shared_ptr<parser::SelectStatement> GetSubselect() { return subselect_; }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
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

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["subselect"] = subselect_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    subselect_ = std::make_shared<parser::SelectStatement>();
    subselect_->FromJson(j.at("subselect"));
  }

 private:
  /**
   * Sub-Select statement
   */
  std::shared_ptr<parser::SelectStatement> subselect_;
};

DEFINE_JSON_DECLARATIONS(SubqueryExpression);

}  // namespace terrier::parser
