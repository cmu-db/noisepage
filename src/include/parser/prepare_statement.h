#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
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
  PrepareStatement(std::string name, common::ManagedPointer<SQLStatement> query,
                   std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders)
      : SQLStatement(StatementType::PREPARE),
        name_(std::move(name)),
        query_(query),
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
  common::ManagedPointer<SQLStatement> GetQuery() { return query_; }

  /**
   * @return number of placeholders
   */
  size_t GetPlaceholdersSize() const { return placeholders_.size(); }

  /**
   * @param idx index of placeholder
   * @return placeholder
   */
  common::ManagedPointer<ParameterValueExpression> GetPlaceholder(size_t idx) { return placeholders_[idx]; }

 private:
  std::string name_;
  common::ManagedPointer<SQLStatement> query_;
  std::vector<common::ManagedPointer<ParameterValueExpression>> placeholders_;
};

}  // namespace parser
}  // namespace terrier
