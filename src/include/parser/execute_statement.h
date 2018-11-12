#pragma once

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
  ExecuteStatement(std::string name, std::vector<std::unique_ptr<AbstractExpression>> parameters)
      : SQLStatement(StatementType::EXECUTE), name_(std::move(name)), parameters_(std::move(parameters)) {}

  ~ExecuteStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string name_;
  const std::vector<std::unique_ptr<AbstractExpression>> parameters_;
};

}  // namespace parser
}  // namespace terrier
