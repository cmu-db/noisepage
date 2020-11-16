#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "parser/sql_statement.h"

namespace noisepage {
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
  ExecuteStatement(std::string name, std::vector<common::ManagedPointer<AbstractExpression>> parameters)
      : SQLStatement(StatementType::EXECUTE), name_(std::move(name)), parameters_(std::move(parameters)) {}

  ~ExecuteStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return execute statement name */
  std::string GetName() { return name_; }

  /** @return execute statement parameters */
  const std::vector<common::ManagedPointer<AbstractExpression>> &GetParameters() const { return parameters_; }

 private:
  const std::string name_;
  const std::vector<common::ManagedPointer<AbstractExpression>> parameters_;
};

}  // namespace parser
}  // namespace noisepage
