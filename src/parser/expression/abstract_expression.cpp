#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/star_expression.h"

namespace terrier::parser {

/**
 * Derived expressions should call this base method
 * @return expression serialized to json
 */
nlohmann::json AbstractExpression::ToJson() const {
  nlohmann::json j;
  j["expression_type"] = expression_type_;
  j["return_value_type"] = return_value_type_;
  j["children"] = children_;
  return j;
}

/**
 * Derived expressions should call this base method
 * @param j json to deserialize
 */
void AbstractExpression::FromJson(const nlohmann::json &j) {
  expression_type_ = j.at("expression_type").get<ExpressionType>();
  return_value_type_ = j.at("return_value_type").get<type::TypeId>();
  children_ = {};

  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    children_.push_back(DeserializeExpression(child_json));
  }
}

std::shared_ptr<AbstractExpression> DeserializeExpression(const nlohmann::json &j) {
  std::shared_ptr<AbstractExpression> expr;

  auto expression_type = j.at("expression_type").get<ExpressionType>();
  switch (expression_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_AVG: {
      expr = std::make_shared<AggregateExpression>();
      break;
    }

    case ExpressionType::STAR: {
      expr = std::make_shared<StarExpression>();
      break;
    }

    default:
      // This is 100% a hack, remove later
      TERRIER_ASSERT(false, "Unknown expression type during deserialization");
  }
  expr->FromJson(j);
  return expr;
}

}  // namespace terrier::parser