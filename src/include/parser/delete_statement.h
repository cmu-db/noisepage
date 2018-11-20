#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier::parser {

/**
 * @class DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
class DeleteStatement : public SQLStatement {
 public:
  DeleteStatement(std::unique_ptr<TableRef> table, std::unique_ptr<AbstractExpression> expr)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(std::move(expr)) {}

  explicit DeleteStatement(std::unique_ptr<TableRef> table)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(nullptr) {}

  DeleteStatement() : SQLStatement(StatementType::DELETE), table_ref_(nullptr), expr_(nullptr) {}

  ~DeleteStatement() override = default;

  std::string GetTableName() const { return table_ref_->GetTableName(); }

  /*
  inline void TryBindDatabaseName(std::string default_database_name) {
    if (table_ref != nullptr)
      table_ref->TryBindDatabaseName(default_database_name);
  }
  */

  // std::string GetDatabaseName() const { return table_ref_->GetDatabaseName(); }

  // std::string GetSchemaName() const { return table_ref_->GetSchemaName(); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  std::unique_ptr<TableRef> table_ref_;
  std::unique_ptr<AbstractExpression> expr_;
};

}  // namespace terrier::parser
