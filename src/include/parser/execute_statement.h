#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
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
  ExecuteStatement(std::string name, std::vector<common::ManagedPointer<AbstractExpression>> parameters)
      : SQLStatement(StatementType::EXECUTE), name_(std::move(name)), parameters_(std::move(parameters)) {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return execute statement name
   */
  std::string GetName() { return name_; }

  /**
   * @return number of execute statement parameters
   */
  size_t GetParametersSize() const { return parameters_.size(); }

  /**
   * @param idx index of parameter
   * @return execute statement parameter
   */
  common::ManagedPointer<AbstractExpression> GetParameter(size_t idx) {
    TERRIER_ASSERT(idx < GetParametersSize(), "Index must be less than number of parameters");
    return parameters_[idx];
  }

 private:
  const std::string name_;
  const std::vector<common::ManagedPointer<AbstractExpression>> parameters_;
};

}  // namespace parser
}  // namespace terrier
