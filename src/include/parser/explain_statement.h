#pragma once

#include <memory>
#include <utility>

#include "parser/sql_statement.h"

namespace noisepage::parser {

/**
 * Represents the SQL "EXPLAIN ..."
 */
class ExplainStatement : public SQLStatement {
 public:
  /** @param real_sql_stmt the SQL statement to be explained */
  explicit ExplainStatement(std::unique_ptr<SQLStatement> real_sql_stmt)
      : SQLStatement(StatementType::EXPLAIN), real_sql_stmt_(std::move(real_sql_stmt)) {}

  ~ExplainStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return the SQL statement to be explained */
  common::ManagedPointer<SQLStatement> GetSQLStatement() { return common::ManagedPointer(real_sql_stmt_); }

 private:
  std::unique_ptr<SQLStatement> real_sql_stmt_;
};

}  // namespace noisepage::parser
