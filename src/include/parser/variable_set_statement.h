#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {
/**
 * Not sure what this is for. Inherited from old codebase.
 */
class VariableSetStatement : public SQLStatement {
 public:
  VariableSetStatement(std::string parameter_name, std::vector<common::ManagedPointer<AbstractExpression>> values)
      : SQLStatement(StatementType::VARIABLE_SET),
        parameter_name_(std::move(parameter_name)),
        values_(std::move(values)) {}

  ~VariableSetStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return The parameter name. */
  std::string GetParameterName() { return parameter_name_; }

  /** @return The set statement values. */
  std::vector<common::ManagedPointer<AbstractExpression>> GetValues() const {
    std::vector<common::ManagedPointer<AbstractExpression>> values;
    values.reserve(values_.size());
    for (const auto &value : values_) {
      values.emplace_back(common::ManagedPointer(value));
    }
    return values;
  }

 private:
  const std::string parameter_name_;
  const std::vector<common::ManagedPointer<AbstractExpression>> values_;
};
}  // namespace parser
}  // namespace terrier
