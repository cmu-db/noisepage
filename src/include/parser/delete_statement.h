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
 * @brief
 * DELETE FROM "tablename";
 *   All records are deleted from table "tablename"
 *
 * DELETE FROM "tablename" WHERE grade > 3.0;
 *   e.g.
 *   DELETE FROM student_grades WHERE grade > 3.0;
 */
class DeleteStatement : public SQLStatement {
 public:
  /**
   * Delete all rows which match expr.
   * @param table deletion target
   * @param expr condition for deletion
   */
  DeleteStatement(std::unique_ptr<TableRef> table, common::ManagedPointer<AbstractExpression> expr)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(expr) {}

  /**
   * Delete all rows (truncate).
   * @param table deletion target
   */
  explicit DeleteStatement(std::unique_ptr<TableRef> table)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(nullptr) {}

  ~DeleteStatement() override = default;

  /** @return deletion target table */
  common::ManagedPointer<TableRef> GetDeletionTable() const { return common::ManagedPointer(table_ref_); }

  /** @return expression that represents deletion condition */
  common::ManagedPointer<AbstractExpression> GetDeleteCondition() { return expr_; }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

 private:
  const std::unique_ptr<TableRef> table_ref_;
  const common::ManagedPointer<AbstractExpression> expr_;
};

}  // namespace terrier::parser
