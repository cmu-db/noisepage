#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace noisepage {
namespace parser {
/**
 * VariableShowStatement represents SQL statements of the form "SHOW ...".
 */
class VariableShowStatement : public SQLStatement {
 public:
  /**
   * @param name The name of the parameter to show.
   */
  explicit VariableShowStatement(std::string name)
      : SQLStatement(StatementType::VARIABLE_SHOW), name_(std::move(name)) {}

  ~VariableShowStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return The name of the variable to show. */
  const std::string &GetName() { return name_; }

 private:
  const std::string name_;
};
}  // namespace parser
}  // namespace noisepage
