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
  std::shared_ptr<parser::SelectStatement> subselect_;
};

DEFINE_JSON_DECLARATIONS(SubqueryExpression);

}  // namespace terrier::parser
