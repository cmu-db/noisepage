#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * Represents a logical function expression.
 */
class SqlFunctionExpression : public SqlAbstractExpression {
 public:
  /**
   * Instantiate a new function expression with the given name and children.
   * @param func_name function name
   * @param return_value_type function return value type
   * @param children children arguments for the function
   */
  SqlFunctionExpression(std::string &&func_name, const type::TypeId return_value_type,
                        std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(parser::ExpressionType::FUNCTION, return_value_type, std::move(children)),
        func_name_(std::move(func_name)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<SqlFunctionExpression>(*this); }

  /**
   * @return function name
   */
  const std::string &GetFuncName() const { return func_name_; }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetFuncName(std::string func_name) {
      func_name_ = func_name;
      return *this;
    }

    std::shared_ptr<SqlFunctionExpression> Build() {
      return std::shared_ptr<SqlFunctionExpression>(new SqlFunctionExpression(func_name_, return_value_type_, std::move(children_)));
    }

   private:
    std::string func_name_;
  };
  friend class Builder;

 private:
  std::string func_name_;
};

}  // namespace terrier::sql
