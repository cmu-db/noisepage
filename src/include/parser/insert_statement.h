#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/sql_node_visitor.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * Represents the sql "INSERT ..."
 */
class InsertStatement : public SQLStatement {
 public:
  /**
   * Insert from SELECT
   * @param columns columns to insert into
   * @param table_ref table
   * @param select select statement to insert from
   */
  InsertStatement(std::vector<std::string> columns, std::shared_ptr<TableRef> table_ref,
                  std::shared_ptr<SelectStatement> select)
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
  InsertStatement(std::vector<std::string> columns, std::shared_ptr<TableRef> table_ref,
                  std::vector<std::vector<const AbstractExpression *>> insert_values)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::VALUES),
        columns_(std::move(columns)),
        table_ref_(std::move(table_ref)),
        insert_values_(std::move(insert_values)) {}

  /**
   * @param type insert type (SELECT or VALUES)
   */
  explicit InsertStatement(InsertType type) : SQLStatement(StatementType::INSERT), type_(type) {}

  ~InsertStatement() override {
    for (auto &tuple : insert_values_) {
      for (auto *value : tuple) {
        delete value;
      }
    }
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return type of insertion
   */
  InsertType GetInsertType() { return type_; }

  /**
   * @return columns to insert into
   */
  std::vector<std::string> GetInsertColumns() { return columns_; }

  /**
   * @return table to insert into
   */
  std::shared_ptr<TableRef> GetInsertionTable() const { return table_ref_; }

  /**
   * @return select statement we're inserting from
   */
  std::shared_ptr<SelectStatement> GetSelect() const { return select_; }

  /**
   * @return values that we're inserting
   */
  std::vector<std::vector<const AbstractExpression *>> GetValues() { return insert_values_; }

 private:
  const InsertType type_;
  const std::vector<std::string> columns_;
  const std::shared_ptr<TableRef> table_ref_;
  const std::shared_ptr<SelectStatement> select_;
  // TODO(WAN): unsure about this one.
  const std::vector<std::vector<const AbstractExpression *>> insert_values_;
};

}  // namespace parser
}  // namespace terrier
