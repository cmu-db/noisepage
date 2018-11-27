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
 * Represents the SQL "PREPARE ..."
 */
class PrepareStatement : public SQLStatement {
 public:
  /**
   * @param name prepared statement name
   * @param query query to be prepared
   * @param placeholders placeholder values
   */
  PrepareStatement(std::string name, std::shared_ptr<SQLStatement> query,
                   std::vector<std::shared_ptr<ParameterValueExpression>> placeholders)
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
   * @return placeholders
   */
  std::vector<std::shared_ptr<ParameterValueExpression>> GetPlaceholders() { return placeholders_; }

 private:
  const std::string name_;
  const std::shared_ptr<SQLStatement> query_;
  const std::vector<std::shared_ptr<ParameterValueExpression>> placeholders_;
};

}  // namespace parser
}  // namespace terrier
