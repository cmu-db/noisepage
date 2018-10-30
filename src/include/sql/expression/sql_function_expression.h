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

 private:
  std::string func_name_;
};

}  // namespace terrier::sql
