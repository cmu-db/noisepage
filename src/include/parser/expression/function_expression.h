#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a logical function expression.
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

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<FunctionExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return function name
   */
  const std::string &GetFuncName() const { return func_name_; }

 private:
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

}  // namespace terrier::parser
