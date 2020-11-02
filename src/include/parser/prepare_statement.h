#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace noisepage {
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
   * @param placeholders - placeholder values? (explain)
   */
  PrepareStatement(std::string name, std::unique_ptr<SQLStatement> query,
                   std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders)
      : SQLStatement(StatementType::PREPARE),
        name_(std::move(name)),
        query_(std::move(query)),
        placeholders_(std::move(placeholders)) {}

  ~PrepareStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return prepared statement name */
  std::string GetName() { return name_; }

  /** @return query */
  common::ManagedPointer<SQLStatement> GetQuery() { return common::ManagedPointer(query_); }

  /** @return placeholders */
  const std::vector<common::ManagedPointer<ParameterValueExpression>> &GetPlaceholders() { return placeholders_; }

 private:
  const std::string name_;
  const std::unique_ptr<SQLStatement> query_;
  const std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders_;
};

}  // namespace parser
}  // namespace noisepage
