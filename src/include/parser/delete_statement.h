#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/managed_pointer.h"
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
  DeleteStatement(common::ManagedPointer<TableRef> table, common::ManagedPointer<AbstractExpression> expr)
      : SQLStatement(StatementType::DELETE), table_ref_(table), expr_(expr) {}

  /**
   * Delete all rows (truncate).
   * @param table deletion target
   */
  explicit DeleteStatement(common::ManagedPointer<TableRef> table)
      : SQLStatement(StatementType::DELETE),
        table_ref_(table),
        expr_(common::ManagedPointer<AbstractExpression>(nullptr)) {}

  /**
   * @return deletion target table
   */
  common::ManagedPointer<TableRef> GetDeletionTable() const { return table_ref_; }

  /**
   * @return expression that represents deletion condition
   */
  common::ManagedPointer<AbstractExpression> GetDeleteCondition() { return expr_; }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  const common::ManagedPointer<TableRef> table_ref_;
  const common::ManagedPointer<AbstractExpression> expr_;
};

}  // namespace terrier::parser
