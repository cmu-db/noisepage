#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * FunctionExpression represents a function invocation (except for CAST(), which is a TypeCastExpression).
 */
class FunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new function expression with the given name and children.
   * @param func_name function name
   * @param return_value_type function return value type
   * @param children children arguments for the function
   */
  FunctionExpression(std::string &&func_name, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::FUNCTION, return_value_type, std::move(children)),
        func_name_(std::move(func_name)) {}

  /** Default constructor for deserialization. */
  FunctionExpression() = default;

  std::unique_ptr<AbstractExpression> Copy() const override {
    std::string func_name = GetFuncName();
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    auto expr = std::make_unique<FunctionExpression>(std::move(func_name), GetReturnValueType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(func_name_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const FunctionExpression &>(rhs);
    return GetFuncName() == other.GetFuncName();
  }

  /** @return function name */
  const std::string &GetFuncName() const { return func_name_; }

  void DeriveExpressionName() override {
    bool first = true;
    std::string name = this->GetFuncName() + "(";
    for (auto &child : this->GetChildren()) {
      if (!first) name.append(",");
      child->DeriveExpressionName();
      name.append(child->GetExpressionName());
      first = false;
    }
    name.append(")");
    this->SetExpressionName(name);
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["func_name"] = func_name_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    func_name_ = j.at("func_name").get<std::string>();
    return exprs;
  }

 private:
  /** Name of function to be invoked. */
  std::string func_name_;

  // To quote Tianyu, "Parsers are dumb. They don't know shit."
  // We should keep it that way, resist adding codegen hacks here.
};

DEFINE_JSON_DECLARATIONS(FunctionExpression);

}  // namespace terrier::parser
