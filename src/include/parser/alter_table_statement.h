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
   * Alter table command type (currently supported):
   * Add a column, drop a column, change default value (and possibly type) of column, change type of column
   *
   */
  enum class AlterType { AddColumn, DropColumn, ColumnDefault, AlterColumnType };

  class AlterTableCmd {
   public:
    /**
     * Add Column
     * @param col_def column definition
     * @param col_name column name
     * @param if_exists IF table EXISTS
     */
    AlterTableCmd(std::unique_ptr<ColumnDefinition> col_def, std::string col_name, bool if_exists)
        : type_(AlterType::AddColumn),
          col_name_(std::move(col_name)),
          col_(std::move(col_def)),
          if_exists_(if_exists) {}

    /**
     * Change Column Type
     * @param col_def column definition
     * @param col_name column name
     */
    AlterTableCmd(std::unique_ptr<ColumnDefinition> col_def, std::string col_name)
        : type_(AlterType::AlterColumnType), col_name_(std::move(col_name)), col_(std::move(col_def)) {}

    /**
     * Set default value
     * @param col_name column name
     * @param default_value default value of column
     */
    AlterTableCmd(std::string col_name, common::ManagedPointer<AbstractExpression> default_value, bool drop_cascade)
        : type_(AlterType::ColumnDefault),
          col_name_(std::move(col_name)),
          col_(nullptr),
          drop_cascade_(drop_cascade),
          default_value_(default_value) {}

    /**
     * Drop Column
     * @param col_name column name
     * @param if_exists IF table EXISTS
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

    /**
     * Equality check
     * @param rhs  other command
     * @return true if the two commands are same
     */
    bool operator==(const AlterTableCmd &rhs) const {
      if (type_ != rhs.type_) return false;
      if (col_name_ != rhs.col_name_) return false;
      if (if_exists_ != rhs.if_exists_) return false;
      if (drop_cascade_ != rhs.drop_cascade_) return false;
      if ((!col_ && rhs.col_) || (col_ && rhs.col_ != col_)) return false;
      if ((!default_value_ && rhs.default_value_) || (default_value_ && default_value_ != rhs.default_value_))
        return false;
      return true;
    }

    /**
     * Hash
     * @return hash of the cmd
     */
    const common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(type_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(col_name_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(drop_cascade_));
      // TODO(XC): is there any case when the underlying values should be hashed rather than pointers?
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(col_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(default_value_));

      return hash;
    }

   private:
    AlterType type_;

    const std::string col_name_;

    // For Add Column
    std::unique_ptr<ColumnDefinition> col_ = nullptr;

    // NOTE: Postgresql 9.6 has ADD COLUMN IF NOT EXISTS, but we are not supporting it yet.
    // Drop Column IF table EXISTS
    bool if_exists_ = false;

    // Drop column cascade or not
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
      : TableRefStatement(StatementType::ALTER, std::move(table)),
        cmds_(std::move(cmds)),
        col_oids_(cmds_.size(), catalog::INVALID_COLUMN_OID),
        if_exists_(if_exist) {}

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  ~AlterTableStatement() override = default;

  /**
   * @return actions of the statement
   */
  const std::vector<AlterTableCmd> &GetAlterTableCmds() const { return cmds_; }

  /**
   * @return Oids of columns each action of the statement apply on
   */
  const std::vector<catalog::col_oid_t> &GetColOids() const { return col_oids_; }

  /**
   * @return  have IF EXISTS or not in the SQL
   */
  bool IsIfExists() const { return if_exists_; }

 private:
  friend class binder::BindNodeVisitor;

  void SetColumnOid(uint32_t cmd_idx, catalog::col_oid_t col_oid) {
    TERRIER_ASSERT(cmd_idx < col_oids_.size(), "Column oid does not match existing command.");
    col_oids_[cmd_idx] = col_oid;
  }

  // Column Commands
  std::vector<AlterTableCmd> cmds_;

  // Oids of column
  std::vector<catalog::col_oid_t> col_oids_;

  // ALTER TABLE IF EXISTS
  const bool if_exists_ = false;
};

}  // namespace terrier::parser
