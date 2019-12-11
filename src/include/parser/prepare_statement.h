#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * SQL PREPARE statement
 */
class PrepareStatement : public SQLStatement {
 public:
  /**
   * PREPARE name [(data_type [, ...])] AS statement;
   *
   * where data_type refers to parameters in the statement, e.g. $1
   * statement may be any of SELECT, INSERT, UPDATE, DELETE or VALUES;
   *
   * @param name - name to be given to the prepared statement
   * @param query - the parsed form of statement
   * @param placeholders - placeholder for parameters
   */
  PrepareStatement(std::string name, std::unique_ptr<SQLStatement> query,
                   std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders)
      : SQLStatement(StatementType::PREPARE),
        name_(std::move(name)),
        query_(std::move(query)),
        placeholders_(std::move(placeholders)) {}

  ~PrepareStatement() override = default;

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /** @return prepared statement name */
  std::string GetName() { return name_; }

  /** @return query */
  common::ManagedPointer<SQLStatement> GetQuery() { return common::ManagedPointer(query_); }

  /** @return placeholders */
  common::ManagedPointer<std::vector<common::ManagedPointer<ParameterValueExpression>>> GetPlaceholders() { return common::ManagedPointer(&placeholders_); }

 private:
  std::string name_;
  std::unique_ptr<SQLStatement> query_;
  std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders_;
};

}  // namespace parser
}  // namespace terrier
