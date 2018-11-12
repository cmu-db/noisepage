#pragma once

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * Represents the SQL "DROP ..."
 */
class DropStatement : public TableRefStatement {
 public:
  enum class DropType { kDatabase, kTable, kSchema, kIndex, kView, kPreparedStatement, kTrigger };

  // DROP DATABASE, DROP TABLE
  DropStatement(std::unique_ptr<TableInfo> table_info, DropType type, bool if_exists)
      : TableRefStatement(StatementType::DROP, std::move(table_info)), type_(type), if_exists_(if_exists) {}

  // DROP INDEX
  DropStatement(std::unique_ptr<TableInfo> table_info, std::string index_name)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kIndex),
        index_name_(std::move(index_name)) {}

  // DROP SCHEMA
  DropStatement(std::unique_ptr<TableInfo> table_info, bool if_exists, bool cascade)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kSchema),
        if_exists_(if_exists),
        cascade_(cascade) {}

  // DROP TRIGGER
  // TODO(WAN): this is a hack to give it a different signature from drop index, refactor
  DropStatement(std::unique_ptr<TableInfo> table_info, DropType type, std::string trigger_name)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kTrigger),
        trigger_name_(std::move(trigger_name)) {}

  ~DropStatement() override = default;

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const DropType type_;

  // DROP DATABASE, SCHEMA
  const bool if_exists_ = false;

  // DROP INDEX
  const std::string index_name_;

  // DROP SCHEMA
  const bool cascade_ = false;

  // drop prepared statement
  const std::string prep_stmt_;

  // drop trigger
  const std::string trigger_name_;
};

}  // namespace parser
}  // namespace terrier
