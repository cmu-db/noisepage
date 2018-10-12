#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
namespace expression {

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
  explicit FunctionExpression(std::string &&func_name, const type::TypeId return_value_type,
                              std::vector<std::unique_ptr<AbstractExpression>> *children)
      : AbstractExpression(ExpressionType::FUNCTION, return_value_type, std::move(*children)),
        func_name_(std::move(func_name)) {}

  AbstractExpression *Copy() const override { return new FunctionExpression(*this); }

  /**
   * Returns the name of the function.
   * @return function name
   */
  const std::string &GetFuncName() const { return func_name_; }

 protected:
  // TODO(WAN): clang-tidy wants me to =default, how does that work?
  /**
   * Copy constructs a function expression.
   * @param other function expression to be copied.
   */
  // NOLINTNEXTLINE
  FunctionExpression(const FunctionExpression &other) : AbstractExpression(other), func_name_(other.func_name_) {}

 private:
  std::string func_name_;

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

}  // namespace expression
}  // namespace parser
}  // namespace terrier
