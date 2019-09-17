#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a logical function expression
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
                     std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::FUNCTION, return_value_type, std::move(children)),
        func_name_(std::move(func_name)) {}

  /**
   * Default constructor for deserialization
   */
  FunctionExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<FunctionExpression>(*this); }

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

  /**
   * @return function name
   */
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

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["func_name"] = func_name_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    func_name_ = j.at("func_name").get<std::string>();
  }

 private:
  /**
   * Name of the function
   */
  std::string func_name_;

  // TODO(Tianyu): Why the hell are these things in the parser nodes anyway? Parsers are dumb. They don't know shit.
  // TODO(WAN): doesn't appear in postgres parser code
  // std::vector<TypeId> func_arg_types_;

  // TODO(WAN): until codegen is in.
  // Does it really make sense to store BuiltInFuncType AND name though?
  // Old code already had map name->func
  // std::shared_ptr<codegen::CodeContext> func_context_;
  // function::BuiltInFuncType func_;

  // TODO(WAN): will user defined functions need special treatment?
  // If so, wouldn't it make more sense for them to have their own class?
  // bool is_udf_;
};

DEFINE_JSON_DECLARATIONS(FunctionExpression);

}  // namespace terrier::parser
