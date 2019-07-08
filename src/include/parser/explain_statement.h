#pragma once

#include <memory>
#include <utility>

#include "common/managed_pointer.h"
#include "parser/sql_statement.h"

namespace terrier::parser {

/**
 * Represents the SQL "EXPLAIN ..."
 */
class ExplainStatement : public SQLStatement {
 public:
  /**
   * @param real_sql_stmt the SQL statement to be explained
   */
  explicit ExplainStatement(common::ManagedPointer<SQLStatement> real_sql_stmt)
      : SQLStatement(StatementType::EXPLAIN), real_sql_stmt_(real_sql_stmt) {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return the SQL statement to be explained
   */
  common::ManagedPointer<SQLStatement> GetSQLStatement() { return real_sql_stmt_; }

 private:
  common::ManagedPointer<SQLStatement> real_sql_stmt_;
};

}  // namespace terrier::parser
