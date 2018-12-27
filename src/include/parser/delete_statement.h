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
  DeleteStatement(std::shared_ptr<TableRef> table, std::shared_ptr<AbstractExpression> expr)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(std::move(expr)) {}

  /**
   * Delete all rows (truncate).
   * @param table deletion target
   */
  explicit DeleteStatement(std::shared_ptr<TableRef> table)
      : SQLStatement(StatementType::DELETE), table_ref_(std::move(table)), expr_(nullptr) {}

  ~DeleteStatement() override = default;

  /**
   * @return deletion target table
   */
  std::shared_ptr<TableRef> GetDeletionTable() const { return table_ref_; }

  /**
   * @return expression that represents deletion condition
   */
  std::shared_ptr<AbstractExpression> GetDeleteCondition() { return expr_; }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  std::shared_ptr<TableRef> table_ref_;
  std::shared_ptr<AbstractExpression> expr_;
};

}  // namespace terrier::parser
