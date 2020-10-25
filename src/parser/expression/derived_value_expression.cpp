#include "parser/expression/derived_value_expression.h"

#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> DerivedValueExpression::Copy() const {
  auto expr = std::make_unique<DerivedValueExpression>(GetReturnValueType(), GetTupleIdx(), GetValueIdx());
  expr->SetMutableStateForCopy(*this);
  return expr;
}

common::hash_t DerivedValueExpression::Hash() const {
  common::hash_t hash = AbstractExpression::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(tuple_idx_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(value_idx_));
  return hash;
}

bool DerivedValueExpression::operator==(const AbstractExpression &rhs) const {
  if (!AbstractExpression::operator==(rhs)) return false;
  auto const &other = dynamic_cast<const DerivedValueExpression &>(rhs);
  if (GetTupleIdx() != other.GetTupleIdx()) return false;
  return GetValueIdx() == other.GetValueIdx();
}

nlohmann::json DerivedValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  j["tuple_idx"] = tuple_idx_;
  j["value_idx"] = value_idx_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> DerivedValueExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  tuple_idx_ = j.at("tuple_idx").get<int>();
  value_idx_ = j.at("value_idx").get<int>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DerivedValueExpression);

}  // namespace noisepage::parser
