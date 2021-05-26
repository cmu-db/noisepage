#pragma once

#include <memory>
#include <utility>

#include "parser/sql_statement.h"

namespace noisepage::parser {

/** The format of the explain statement's output. */
enum class ExplainStatementFormat : uint8_t { JSON, TPL, TBC };

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

  /** @brief override the default format of this EXPLAIN statement */
  void SetFormat(const ExplainStatementFormat format) { format_ = format; }

  /** @return the SQL statement to be explained */
  common::ManagedPointer<SQLStatement> GetSQLStatement() const { return common::ManagedPointer(real_sql_stmt_); }

  /** @return format of the EXPLAIN */
  ExplainStatementFormat GetFormat() const { return format_; }

 private:
  std::unique_ptr<SQLStatement> real_sql_stmt_;
  ExplainStatementFormat format_ = ExplainStatementFormat::JSON;  // default to JSON since we rely on serializing the
                                                                  // physical plan via dumping to JSON
};

}  // namespace noisepage::parser
