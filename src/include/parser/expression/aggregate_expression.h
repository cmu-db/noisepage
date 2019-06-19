#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

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
  AggregateExpression(ExpressionType type, std::vector<std::shared_ptr<AbstractExpression>> &&children, bool distinct)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)), distinct_(distinct) {}

  /**
   * Default constructor for deserialization
   */
  AggregateExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<AggregateExpression>(*this); }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const AggregateExpression &>(rhs);
    return IsDistinct() == other.IsDistinct();
  }

  /**
   * @return true if we should eliminate duplicate values in aggregate function calculations
   */
  bool IsDistinct() const { return distinct_; }

  void DeduceExpressionType() override {
    switch (this->GetExpressionType()) {
      case ExpressionType::AGGREGATE_COUNT:
        return_value_type_ = type::TypeId::INTEGER;
        break;
        // return the type of the base
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_SUM:
        TERRIER_ASSERT(this->GetChildrenSize()>= 1, "No column name given.");
        return_value_type_ = this->GetChild(0)->GetReturnValueType();
        break;
      case ExpressionType::AGGREGATE_AVG:
        return_value_type_ = type::TypeId::DECIMAL;
        break;
      default:
        break;
    }
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

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
