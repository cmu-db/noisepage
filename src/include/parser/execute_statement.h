#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * Represents the SQL "EXECUTE ..."
 */
class ExecuteStatement : public SQLStatement {
 public:
  /**
   * @param name name of execute statement
   * @param parameters parameters for execute statement
   */
  ExecuteStatement(std::string name, std::vector<std::shared_ptr<AbstractExpression>> parameters)
      : SQLStatement(StatementType::EXECUTE), name_(std::move(name)), parameters_(std::move(parameters)) {}

  ~ExecuteStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return execute statement name
   */
  std::string GetName() { return name_; }

  /**
   * @return execute statement parameters
   */
  std::vector<std::shared_ptr<AbstractExpression>> GetParameters() { return parameters_; }

 private:
  const std::string name_;
  const std::vector<std::shared_ptr<AbstractExpression>> parameters_;
};

}  // namespace parser
}  // namespace terrier
