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
      : SQLStatement(StatementType::DELETE), from_(std::move(table)), where_(expr) {}

  /**
   * Delete all rows (truncate).
   * @param table deletion target
   */
  explicit DeleteStatement(std::unique_ptr<TableRef> table)
      : SQLStatement(StatementType::DELETE), from_(std::move(table)), where_(nullptr) {}

  ~DeleteStatement() override = default;

  /**
   * @return the hashed value of this DeleteStatement
   */
  common::hash_t Hash() const override {
    common::hash_t hash = SQLStatement::Hash();
    if (from_ != nullptr) hash = common::HashUtil::CombineHashes(hash, from_->Hash());
    if (where_ != nullptr) hash = common::HashUtil::CombineHashes(hash, where_->Hash());
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two DeleteStatements are logically equal
   */
  bool operator==(const SQLStatement &rhs) const override {
    if (!SQLStatement::operator==(rhs)) return false;
    auto const &delete_stmt = dynamic_cast<const DeleteStatement &>(rhs);
    if (from_ != nullptr && delete_stmt.from_ == nullptr) return false;
    if (from_ == nullptr && delete_stmt.from_ != nullptr) return false;
    if (from_ != nullptr && delete_stmt.from_ != nullptr && *(from_) != *(delete_stmt.from_)) return false;

    if (where_ != nullptr && delete_stmt.where_ == nullptr) return false;
    if (where_ == nullptr && delete_stmt.where_ != nullptr) return false;
    if (where_ != nullptr && delete_stmt.where_ != nullptr && *(where_) != *(delete_stmt.where_)) return false;
    return true;
  }

  /** @return deletion target table */
  common::ManagedPointer<TableRef> GetDeletionTable() const { return common::ManagedPointer(from_); }

  /** @return expression that represents deletion condition */
  common::ManagedPointer<AbstractExpression> GetDeleteCondition() { return where_; }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

 private:
  const std::unique_ptr<TableRef> from_;
  const common::ManagedPointer<AbstractExpression> where_;
};

}  // namespace terrier::parser
