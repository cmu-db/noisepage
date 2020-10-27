#include "parser/expression/parameter_value_expression.h"

#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> ParameterValueExpression::Copy() const {
  auto expr = std::make_unique<ParameterValueExpression>(GetValueIdx());
  expr->SetMutableStateForCopy(*this);
  return expr;
}

nlohmann::json ParameterValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  j["value_idx"] = value_idx_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> ParameterValueExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  value_idx_ = j.at("value_idx").get<uint32_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ParameterValueExpression);

}  // namespace noisepage::parser
