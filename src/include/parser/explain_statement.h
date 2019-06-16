#pragma once

#include <memory>
#include <utility>

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
  explicit ExplainStatement(std::shared_ptr<SQLStatement> real_sql_stmt)
      : SQLStatement(StatementType::EXPLAIN), real_sql_stmt_(std::move(real_sql_stmt)) {}
  ~ExplainStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return the SQL statement to be explained
   */
  std::shared_ptr<SQLStatement> GetSQLStatement() { return real_sql_stmt_; }

 private:
  // TODO(Gus): Get rid of shared pointer
  std::shared_ptr<SQLStatement> real_sql_stmt_;
};

}  // namespace terrier::parser
