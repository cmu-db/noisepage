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
  /** @param real_sql_stmt the SQL statement to be explained */
  explicit ExplainStatement(std::unique_ptr<SQLStatement> real_sql_stmt)
      : SQLStatement(StatementType::EXPLAIN), real_sql_stmt_(std::move(real_sql_stmt)) {}

  ~ExplainStatement() override = default;

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /** @return the SQL statement to be explained */
  common::ManagedPointer<SQLStatement> GetSQLStatement() { return common::ManagedPointer(real_sql_stmt_); }

 private:
  std::unique_ptr<SQLStatement> real_sql_stmt_;
};

}  // namespace terrier::parser
