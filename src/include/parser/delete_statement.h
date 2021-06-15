#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace noisepage::parser {
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

  /** @return deletion target table */
  common::ManagedPointer<TableRef> GetDeletionTable() const { return common::ManagedPointer(table_ref_); }

  /** @return expression that represents deletion condition */
  common::ManagedPointer<AbstractExpression> GetDeleteCondition() { return expr_; }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return a collection of the temporary tables available to the DELETE */
  std::vector<common::ManagedPointer<TableRef>> GetDeleteWith() const {
    // TODO(Kyle): This collection is currently NEVER populated
    std::vector<common::ManagedPointer<TableRef>> table_refs{};
    table_refs.reserve(with_tables_.size());
    std::transform(with_tables_.cbegin(), with_tables_.cend(), std::back_inserter(table_refs),
                   [](const auto &ref) { return common::ManagedPointer<TableRef>(ref); });
    return table_refs;
  }

 private:
  const std::unique_ptr<TableRef> table_ref_;
  const common::ManagedPointer<AbstractExpression> expr_;

  std::vector<std::unique_ptr<TableRef>> with_tables_;
};

}  // namespace noisepage::parser
