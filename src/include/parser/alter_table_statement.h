#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "parser/create_statement.h"
#include "parser/sql_statement.h"

namespace terrier::parser {
/**
 * @class AlterTableStatement
 * @brief ALTER TABLE ...
 */
class AlterTableStatement : public TableRefStatement {
 public:
  /**
   * Alter table command type (currently supported)
   */
  enum class AlterType { AddColumn, DropColumn, ColumnDefault, AlterColumnType };

  class AlterTableCmd {
   public:
    /**
     * Add Column
     * @param col_def column definition
     * @param col_name
     * @param if_exists IF EXISTS
     */
    AlterTableCmd(std::unique_ptr<ColumnDefinition> col_def, std::string col_name, bool if_exists)
        : type_(AlterType::AddColumn),
          col_name_(std::move(col_name)),
          col_(std::move(col_def)),
          if_exists_(if_exists) {}

    /**
     * Change Column Type
     * @param col_def
     * @param col_name
     */
    AlterTableCmd(std::unique_ptr<ColumnDefinition> col_def, std::string col_name)
        : type_(AlterType::AlterColumnType), col_name_(std::move(col_name)), col_(std::move(col_def)) {}

    /**
     * Set default value
     * @param col_name
     * @param default_value
     */
    AlterTableCmd(std::string col_name, common::ManagedPointer<AbstractExpression> default_value, bool drop_cascade)
        : type_(AlterType::ColumnDefault),
          col_name_(std::move(col_name)),
          col_(nullptr),
          drop_cascade_(drop_cascade),
          default_value_(default_value) {}

    /**
     * Drop Column
     * @param col_name
     * @param if_exists
     */
    AlterTableCmd(std::string col_name, bool if_exists, bool drop_cascade)
        : type_(AlterType::DropColumn),
          col_name_(std::move(col_name)),
          if_exists_(if_exists),
          drop_cascade_(drop_cascade) {}

    /**
     * @return column definition
     */
    ColumnDefinition &GetColumn() const { return *col_; }

    /**
     * @return  type of the command
     */
    const AlterType GetAlterType() const { return type_; }

    /**
     * @return column name
     */
    const std::string &GetColumnName() const { return col_name_; }

    /**
     * @return default expression
     */
    common::ManagedPointer<AbstractExpression> GetDefaultExpression() const {
      return common::ManagedPointer(default_value_);
    }

    /**
     * @return IF EXISTS
     */
    bool IsIfExists() const { return if_exists_; }

    /**
     * @return If drop cascade
     */
    bool IsDropCascade() const { return drop_cascade_; }

   private:
    AlterType type_;

    const std::string col_name_;

    // For Add Column
    std::unique_ptr<ColumnDefinition> col_;

    // NOTE: Postgresql 9.6 has ADD COLUMN IF NOT EXISTS, but we are not supporting it yet.
    // Drop Column IF EXISTS
    bool if_exists_ = false;

    // Drop default value cascade or not
    bool drop_cascade_ = false;

    // For setting default
    common::ManagedPointer<AbstractExpression> default_value_ = nullptr;
  };

  /**
   *  Constructor
   * @param table  tableInfo reference
   * @param cmds alter table actions
   * @param if_exist IF EXISTS (for the table)
   */
  AlterTableStatement(std::unique_ptr<TableInfo> table, std::vector<AlterTableCmd> cmds, bool if_exist)
      : TableRefStatement(StatementType::ALTER, std::move(table)), cmds_(std::move(cmds)), if_exists_(if_exist) {}

  // TODO(SC)
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  ~AlterTableStatement() override = default;

  /**
   * @return actions of the statement
   */
  const std::vector<AlterTableCmd> &GetAlterTableCmds() const { return cmds_; }

  /**
   * @return  have IF EXISTS or not in the SQL
   */
  bool IsIfExists() const { return if_exists_; }

 private:
  // Column Commands
  const std::vector<AlterTableCmd> cmds_;

  // ALTER TABLE IF EXISTS
  const bool if_exists_ = false;
};

}  // namespace terrier::parser
