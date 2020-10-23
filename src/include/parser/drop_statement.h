#pragma once

#include <memory>
#include <string>
#include <utility>

#include "binder/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace noisepage {
namespace parser {
/**
 * DropStatement represents the SQL "DROP ..."
 */
class DropStatement : public TableRefStatement {
 public:
  /** Drop statement type. */
  enum class DropType { kDatabase, kTable, kSchema, kIndex, kView, kPreparedStatement, kTrigger };

  /**
   * DROP DATABASE, DROP TABLE
   * @param table_info table information
   * @param type kDatabase or kTable
   * @param if_exists true if "IF EXISTS" was used
   */
  DropStatement(std::unique_ptr<TableInfo> table_info, DropType type, bool if_exists)
      : TableRefStatement(StatementType::DROP, std::move(table_info)), type_(type), if_exists_(if_exists) {}

  /**
   * DROP INDEX
   * @param table_info table information
   * @param index_name index name
   */
  DropStatement(std::unique_ptr<TableInfo> table_info, std::string index_name)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kIndex),
        index_name_(std::move(index_name)) {}

  /**
   * DROP SCHEMA
   * @param table_info table information
   * @param if_exists true if "IF EXISTS" was used
   * @param cascade true if "CASCADE" was used
   */
  DropStatement(std::unique_ptr<TableInfo> table_info, bool if_exists, bool cascade)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kSchema),
        if_exists_(if_exists),
        cascade_(cascade) {}

  /**
   * DROP TRIGGER
   * TODO(WAN): this is a hack to get a different signature, type is unnecessary. Refactor into subclass.
   * @param table_info table information
   * @param type kTrigger
   * @param trigger_name trigger name
   */
  DropStatement(std::unique_ptr<TableInfo> table_info, DropType type, std::string trigger_name)
      : TableRefStatement(StatementType::DROP, std::move(table_info)),
        type_(DropType::kTrigger),
        trigger_name_(std::move(trigger_name)) {}

  ~DropStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return drop type */
  DropType GetDropType() { return type_; }

  /** @return true if "IF EXISTS" was used for [DROP DATABASE, DROP SCHEMA] */
  bool IsIfExists() { return if_exists_; }

  /** @return index name for [DROP INDEX] */
  std::string GetIndexName() { return index_name_; }

  /** @return true if "CASCADE" was used for [DROP SCHEMA] */
  bool IsCascade() { return cascade_; }

  /** @return trigger name for [DROP TRIGGER] */
  std::string GetTriggerName() { return trigger_name_; }

 private:
  const DropType type_;

  // DROP DATABASE, SCHEMA
  const bool if_exists_ = false;

  // DROP INDEX
  const std::string index_name_;

  // DROP SCHEMA
  const bool cascade_ = false;

  // DROP TRIGGER
  const std::string trigger_name_;
};

}  // namespace parser
}  // namespace noisepage
