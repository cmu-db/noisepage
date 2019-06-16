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
   * @param placeholders - placeholder values? (explain)
   */
  PrepareStatement(std::string name, std::shared_ptr<SQLStatement> query,
                   std::vector<const ParameterValueExpression *> placeholders)
      : SQLStatement(StatementType::PREPARE),
        name_(std::move(name)),
        query_(std::move(query)),
        placeholders_(std::move(placeholders)) {}

  ~PrepareStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return prepared statement name
   */
  std::string GetName() { return name_; }

  /**
   * @return query
   */
  std::shared_ptr<SQLStatement> GetQuery() { return query_; }

  /**
   * @return number of placholders
   */
  size_t GetPlacerholdersSize() const { return placeholders_.size(); }

  /**
   * @param idx index of placerholder
   * @return placeholder
   */
  common::ManagedPointer<const ParameterValueExpression> GetPlaceholder(size_t idx) {
    return common::ManagedPointer<const ParameterValueExpression>(placeholders_[idx]);
  }

 private:
  const std::string name_;
  const std::shared_ptr<SQLStatement> query_;
  const std::vector<const ParameterValueExpression *> placeholders_;
};

}  // namespace parser
}  // namespace terrier
