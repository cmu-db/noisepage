#include "parser/expression/constant_value_expression.h"
#include "common/json_header.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> ConstantValueExpression::Copy() const {
  auto expr = std::make_unique<ConstantValueExpression>(GetValue());
  expr->SetMutableStateForCopy(*this);
  return expr;
}

void ConstantValueExpression::DeriveExpressionName() {
  if (!this->GetAlias().empty()) {
    this->SetExpressionName(this->GetAlias());
  } else {
    this->SetExpressionName(value_.ToString());
  }
}

nlohmann::json ConstantValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  j["value"] = value_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> ConstantValueExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  value_ = j.at("value").get<type::TransientValue>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
