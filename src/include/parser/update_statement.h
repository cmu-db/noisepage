#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {

/**
 * @struct UpdateClause
 * @brief Represents "column = value" expressions
 */
class UpdateClause {
 public:
  /**
   * @param column column to be updated
   * @param value value to update to
   */
  UpdateClause(std::string column, common::ManagedPointer<AbstractExpression> value)
      : column_(std::move(column)), value_(value) {}
  ~UpdateClause() = default;

  /**
   * @return column to be updated
   */
  std::string GetColumnName() { return column_; }

  /**
   * @return value to update to
   */
  common::ManagedPointer<AbstractExpression> GetUpdateValue() { return value_; }

 private:
  const std::string column_;
  const common::ManagedPointer<AbstractExpression> value_;
};

/**
 * @class UpdateStatement
 * @brief Represents "UPDATE"
 */
class UpdateStatement : public SQLStatement {
 public:
  /**
   * @param table table to be updated
   * @param updates update clauses
   * @param where update conditions
   */
  UpdateStatement(std::unique_ptr<TableRef> table, std::vector<std::unique_ptr<UpdateClause>> updates,
                  common::ManagedPointer<AbstractExpression> where)
      : SQLStatement(StatementType::UPDATE), table_(std::move(table)), updates_(std::move(updates)), where_(where) {}

  UpdateStatement() : SQLStatement(StatementType::UPDATE), table_(nullptr), where_(nullptr) {}

  ~UpdateStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /** @return update table target */
  common::ManagedPointer<TableRef> GetUpdateTable() { return common::ManagedPointer(table_); }

  /** @return update clauses */
  std::vector<common::ManagedPointer<UpdateClause>> GetUpdateClauses() {
    std::vector<common::ManagedPointer<UpdateClause>> updates;
    updates.reserve(updates_.size());
    for (const auto &update : updates_) {
      updates.emplace_back(common::ManagedPointer(update));
    }
    return updates;
  }

  /** @return update condition */
  common::ManagedPointer<AbstractExpression> GetUpdateCondition() { return where_; }

 private:
  const std::unique_ptr<TableRef> table_;
  const std::vector<std::unique_ptr<UpdateClause>> updates_;
  const common::ManagedPointer<AbstractExpression> where_;
};

}  // namespace parser
}  // namespace terrier
