#pragma once
#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {
/**
 * ParameterValueExpression represents a parameter's offset in an expression.
 * TODO(WAN): give an example. I believe this is 0-indexed, look at ParamRefTransform code path. Good beginner task?
 */
class ParameterValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new ParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   */
  explicit ParameterValueExpression(const uint32_t value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, type::TypeId::INTEGER, {}), value_idx_(value_idx) {}

  /** Default constructor for deserialization. */
  ParameterValueExpression() = default;

  std::unique_ptr<AbstractExpression> Copy() const override {
    auto expr = std::make_unique<ParameterValueExpression>(GetValueIdx());
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  /** @return offset in the expression */
  uint32_t GetValueIdx() const { return value_idx_; }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(value_idx_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const ParameterValueExpression &>(rhs);
    return GetValueIdx() == other.GetValueIdx();
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["value_idx"] = value_idx_;
    return j;
  }

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    value_idx_ = j.at("value_idx").get<uint32_t>();
    return exprs;
  }

 private:
  // TODO(Tianyu): Can we get a better name for this?
  /** Offset of the value that this expression points to in the query's parameter list. */
  uint32_t value_idx_;
};

DEFINE_JSON_DECLARATIONS(ParameterValueExpression);

}  // namespace terrier::parser
