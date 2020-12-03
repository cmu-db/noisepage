#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace noisepage {
namespace parser {
/**
 * InsertStatement represents the sql "INSERT ..."
 */
class InsertStatement : public SQLStatement {
 public:
  /**
   * Insert from SELECT
   * @param columns columns to insert into
   * @param table_ref table
   * @param select select statement to insert from
   */
  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<SelectStatement> select)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::SELECT),
        columns_(std::move(columns)),
        table_ref_(std::move(table_ref)),
        select_(std::move(select)) {}

  /**
   * Insert from VALUES
   * @param columns columns to insert into
   * @param table_ref table
   * @param insert_values values to be inserted
   */
  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> insert_values)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::VALUES),
        columns_(std::move(columns)),
        table_ref_(std::move(table_ref)),
        insert_values_(std::move(insert_values)) {}

  /** @param type insert type (SELECT or VALUES) */
  explicit InsertStatement(InsertType type) : SQLStatement(StatementType::INSERT), type_(type) {}

  ~InsertStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return type of insertion */
  InsertType GetInsertType() { return type_; }

  /** @return columns to insert into */
  common::ManagedPointer<std::vector<std::string>> GetInsertColumns() { return common::ManagedPointer(columns_); }

  /** @return table to insert into */
  common::ManagedPointer<TableRef> GetInsertionTable() const { return common::ManagedPointer(table_ref_); }

  /** @return select statement we're inserting from */
  common::ManagedPointer<SelectStatement> GetSelect() const { return common::ManagedPointer(select_); }

  /** @return values that we're inserting */
  common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> GetValues() {
    return common::ManagedPointer(insert_values_);
  }

 private:
  const InsertType type_;
  const std::unique_ptr<std::vector<std::string>> columns_;
  const std::unique_ptr<TableRef> table_ref_;
  const std::unique_ptr<SelectStatement> select_;
  const std::unique_ptr<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> insert_values_;
};

}  // namespace parser
}  // namespace noisepage
