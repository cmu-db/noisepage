#pragma once

#include "parser/sql_statement.h"

namespace terrier::parser {

/**
 * Represents the SQL "EXPLAIN ..."
 */
class ExplainStatement : public SQLStatement {
 public:
  ExplainStatement(std::unique_ptr<SQLStatement> real_sql_stmt)
      : SQLStatement(StatementType::EXPLAIN), real_sql_stmt_(std::move(real_sql_stmt)) {}
  ~ExplainStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  std::unique_ptr<SQLStatement> real_sql_stmt_;
};

}  // namespace terrier::parser

//namespace terrier::parser {
//
///**
// * @class ExplainStatement
// * @brief Represents "EXPLAIN <query>"
// */
//class ExplainStatement : public SQLStatement {
// public:
//  ExplainStatement(std::unique_ptr<SQLStatement> query)
//    : SQLStatement(StatementType::EXPLAIN),
//      real_sql_stmt_(std::move(query)) {}
//  
//  ExplainStatement() : SQLStatement(StatementType::EXPLAIN) {}
//  virtual ~ExplainStatement() {}
//
//  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
//
//  std::unique_ptr<parser::SQLStatement> real_sql_stmt_;
//};
//
//}  // namespace terrier::parser

