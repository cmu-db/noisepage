#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   * @param distinct whether to eliminate duplicate values in aggregate function calculations
   */
  AggregateExpression(ExpressionType type, std::vector<const AbstractExpression *> children, bool distinct)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)), distinct_(distinct) {}

  /**
   * Default constructor for deserialization
   */
  AggregateExpression() = default;

  ~AggregateExpression() override = default;

  const AbstractExpression *Copy() const override {
    std::vector<const AbstractExpression *> children;
    for (const auto *child : children_) {
      children.emplace_back(child->Copy());
    }
    return new AggregateExpression(GetExpressionType(), children, distinct_);
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const AggregateExpression &>(rhs);
    return IsDistinct() == other.IsDistinct();
  }

  /**
   * @return true if we should eliminate duplicate values in aggregate function calculations
   */
  bool IsDistinct() const { return distinct_; }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["distinct"] = distinct_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    distinct_ = j.at("distinct").get<bool>();
  }

 private:
  bool distinct_;
};

DEFINE_JSON_DECLARATIONS(AggregateExpression);

}  // namespace terrier::parser
