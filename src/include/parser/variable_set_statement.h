#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace noisepage {
namespace parser {
/**
 * Not sure what this is for. Inherited from old codebase.
 */
class VariableSetStatement : public SQLStatement {
 public:
  /**
   * @param parameter_name The name of the parameter.
   * @param values The values to set in the parameter.
   * @param is_set_default True if the parameter should be set to DEFAULT, in which case values should be empty.
   */
  VariableSetStatement(std::string parameter_name, std::vector<common::ManagedPointer<AbstractExpression>> values,
                       bool is_set_default)
      : SQLStatement(StatementType::VARIABLE_SET),
        parameter_name_(std::move(parameter_name)),
        values_(std::move(values)),
        is_set_default_(is_set_default) {
    NOISEPAGE_ASSERT((values_.empty() && is_set_default_) || (values_.size() == 1 && !is_set_default_),
                     "There is only support for setting one value or setting to default.");
  }

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

  /** @return True if the parameter should be set to DEFAULT. */
  bool IsSetDefault() const { return is_set_default_; }

 private:
  const std::string parameter_name_;
  const std::vector<common::ManagedPointer<AbstractExpression>> values_;
  bool is_set_default_;
};
}  // namespace parser
}  // namespace noisepage
