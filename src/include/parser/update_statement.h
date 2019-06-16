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
  UpdateClause(std::string column, const AbstractExpression *value) : column_(std::move(column)), value_(value) {}

  ~UpdateClause() { delete value_; }

  /**
   * @return column to be updated
   */
  std::string GetColumnName() { return column_; }

  /**
   * @return value to update to
   */
  common::ManagedPointer<const AbstractExpression> GetUpdateValue() {
    return common::ManagedPointer<const AbstractExpression>(value_);
  }

 private:
  const std::string column_;
  const AbstractExpression *value_;
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
  UpdateStatement(std::shared_ptr<TableRef> table, std::vector<std::shared_ptr<UpdateClause>> updates,
                  const AbstractExpression *where)
      : SQLStatement(StatementType::UPDATE), table_(std::move(table)), updates_(std::move(updates)), where_(where) {}

  UpdateStatement() : SQLStatement(StatementType::UPDATE), table_(nullptr) {}

  ~UpdateStatement() override { delete where_; }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return update table target
   */
  std::shared_ptr<TableRef> GetUpdateTable() { return table_; }

  /**
   * @return update clauses
   */
  std::vector<std::shared_ptr<UpdateClause>> GetUpdateClauses() { return updates_; }

  /**
   * @return update condition
   */
  common::ManagedPointer<const AbstractExpression> GetUpdateCondition() {
    return common::ManagedPointer<const AbstractExpression>(where_);
  }

 private:
  const std::shared_ptr<TableRef> table_;
  const std::vector<std::shared_ptr<UpdateClause>> updates_;
  const AbstractExpression *where_ = nullptr;
};

}  // namespace parser
}  // namespace terrier
