#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/alter_table_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::planner {

/**
 * Base class for the ALTER TABLE command, that is passed to the execution engine
 */
class AlterCmdBase {
 public:
  /**
   * default constructor
   */
  AlterCmdBase() = default;

  /**
   * constructor for AlterCmdBase
   * @param oid The col oid that the command applies on
   */
  explicit AlterCmdBase(catalog::col_oid_t oid) : oid_(oid) {}

  /**
   * default destructor
   */
  virtual ~AlterCmdBase() = default;

  /**
   * @return the type of the command
   */
  virtual parser::AlterTableStatement::AlterType GetType() const = 0;

  /**
   * @return the oid of the column
   */
  catalog::col_oid_t GetColOid() const { return oid_; }

  /**
   * @return  json encoded
   */
  virtual nlohmann::json ToJson() const = 0;

  /**
   * Deserializes the command
   * @param j serialized json of the AlterCmdBase
   */
  virtual void FromJson(const nlohmann::json &j) = 0;

  /**
   * @param rhs other ALTER TABLE command base class
   * @return whether two ALTER TABLE command base classes are equal
   */
  virtual bool operator==(const AlterCmdBase &rhs) const {
    if (GetType() != rhs.GetType()) return false;
    if (oid_ != rhs.oid_) return false;
    return true;
  }

 private:
  /**
   * The col oid that the command applies on
   */
  catalog::col_oid_t oid_;
};

/**
 * The plan node for ALTER
 */
class AlterPlanNode : public AbstractPlanNode {
 public:
  class AddColumnCmd : public AlterCmdBase {
   public:
    AddColumnCmd() = default;

    /**
     * Constructor for AddColumnCmd
     * @param col new column to be added
     * @param foreign_key foreign key information
     * @param con_unique unique constraint
     * @param con_check check constraint
     */
    AddColumnCmd(catalog::Schema::Column &&col, std::unique_ptr<ForeignKeyInfo> foreign_key,
                 std::unique_ptr<UniqueInfo> con_unique, std::unique_ptr<CheckInfo> con_check)
        : AlterCmdBase(col.Oid()),
          col_(col),
          foreign_key_(std::move(foreign_key)),
          con_unique_(std::move(con_unique)),
          con_check_(std::move(con_check)) {}

    // TODO(SC)
    /**
     * @return  serialized AddColumnCmd
     */
    nlohmann::json ToJson() const override {
      nlohmann::json j;
      return j;
    }

    /**
     * @return type of this ALTER TABLE cmd
     */
    parser::AlterTableStatement::AlterType GetType() const override {
      return parser::AlterTableStatement::AlterType::AddColumn;
    }

    /**
     * @return new column to be added
     */
    const catalog::Schema::Column &GetColumn() const { return col_; }

    // TODO(SC)
    /**
     *  Deserializes an AddColumnCmd
     * @param j serialized json of the AlterCmdBase
     */
    void FromJson(const nlohmann::json &j) override { (void)j; }

    /**
     * @param rhs other AddColumnCmd
     * @return whether the two AddColumnCmds are equal
     */
    bool operator==(const AlterCmdBase &rhs) const override {
      if (!AlterCmdBase::operator==(rhs)) return false;
      auto &other = dynamic_cast<const AddColumnCmd &>(rhs);
      if (col_ != other.col_) return false;
      if (!(foreign_key_ == other.foreign_key_)) return false;
      if (!(con_unique_ == other.con_unique_)) return false;
      if (!(con_check_ == other.con_check_)) return false;
      return true;
    }

   private:
    /**
     * New Column to be addded
     */
    catalog::Schema::Column col_;

    /**
     * Foreign Key information
     */
    std::unique_ptr<ForeignKeyInfo> foreign_key_ = nullptr;

    /**
     * Unique Constraint
     */
    std::unique_ptr<UniqueInfo> con_unique_ = nullptr;

    /**
     * Check constraint
     */
    std::unique_ptr<CheckInfo> con_check_ = nullptr;
  };

  /**
   * Drop Column command
   */
  class DropColumnCmd : public AlterCmdBase {
   public:
    DropColumnCmd() = default;

    /**
     * Constructor
     */
    DropColumnCmd(std::string col_name, bool is_exist, bool drop_cascade, catalog::col_oid_t oid)
        : AlterCmdBase(oid), col_name_(std::move(col_name)), is_exist_(is_exist), drop_cascade_(drop_cascade) {}

    /**
     * @return name of the column to drop
     */
    const std::string &GetName() const { return col_name_; }

    /**
     * @return  IF EXIST option
     */
    bool IsIfExist() const { return is_exist_; }

    /**
     * @return CASCADE option
     */
    bool IsCascade() const { return drop_cascade_; }

    /**
     * @return  serialized the command
     */
    nlohmann::json ToJson() const override {
      // TODO(XC)
      nlohmann::json j;
      return j;
    }

    /**
     * @return type of this ALTER TABLE cmd
     */
    parser::AlterTableStatement::AlterType GetType() const override {
      return parser::AlterTableStatement::AlterType::DropColumn;
    }

    // TODO(XC)
    /**
     *  Deserializes an AddColumnCmd
     * @param j serialized json of the AlterCmdBase
     */
    void FromJson(const nlohmann::json &j) override { (void)j; }

    /**
     * @param rhs other cmd
     * @return whether the two DropColumnCmds are equal
     */
    bool operator==(const AlterCmdBase &rhs) const override {
      if (!AlterCmdBase::operator==(rhs)) return false;
      auto &other = dynamic_cast<const DropColumnCmd &>(rhs);
      if (col_name_ != other.col_name_) return false;
      if (is_exist_ != other.is_exist_) return false;
      if (drop_cascade_ != other.drop_cascade_) return false;
      return true;
    }

   private:
    // Col to drop
    std::string col_name_;

    // IF EXIST option
    bool is_exist_;

    // Drop cascade option
    bool drop_cascade_;
  };

  /**
   * Builder for an alter plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid OID of the database
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param column_oids OIDs of the columns of the target table
     * @return builder object
     */
    Builder &SetColumnOIDs(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = std::move(column_oids);
      return *this;
    }

    /**
     * @param cmds cmds within ALTER TABLE
     * @return builder object
     */
    Builder &SetCommands(std::vector<std::unique_ptr<AlterCmdBase>> &&cmds) {
      cmds_ = std::move(cmds);
      return *this;
    }

    /**
     * Build the alter plan node
     * @return plan node
     */
    std::unique_ptr<AlterPlanNode> Build() {
      return std::unique_ptr<AlterPlanNode>(new AlterPlanNode(std::move(children_), std::move(output_schema_),
                                                              table_oid_, std::move(column_oids_), std::move(cmds_)));
    }

    /**
     * Extract unique constraint
     * @param col_def column definition
     * @return unique info
     */
    std::unique_ptr<UniqueInfo> ProcessUniqueConstraint(const parser::ColumnDefinition &col_def) {
      // TODO(XC): after we are parsing this
      return nullptr;
    }

    /**
     * Extract Check constraint
     * @param col_def column definition
     * @return check info
     */
    std::unique_ptr<CheckInfo> ProcessCheckConstraint(const parser::ColumnDefinition &col_def) {
      // TODO(XC): after we are parsing this
      return nullptr;
    }

    /**
     * Extract foreign key info
     * @param col_def column definition
     * @return foreignkey info
     */
    std::unique_ptr<ForeignKeyInfo> ProcessForeignKeyConstraint(const parser::ColumnDefinition &col_def) {
      // TODO(XC): after we are parsing this
      return nullptr;
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of the target table
     */
    catalog::table_oid_t table_oid_;

    /**
     * oids of the columns to be analyzed
     */
    std::vector<catalog::col_oid_t> column_oids_;

    /**
     * commands
     */
    std::vector<std::unique_ptr<AlterCmdBase>> cmds_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param table_oid OID of the target SQL table
   * @param column_oids OIDs of the columns of the target table
   */
  AlterPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&column_oids,
                std::vector<std::unique_ptr<AlterCmdBase>> &&cmds)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        table_oid_(table_oid),
        column_oids_(std::move(column_oids)),
        cmds_(std::move(cmds)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  AlterPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AlterPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ALTER; }

  /**
   * @return the OID of the target table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the OIDs of the columns to be analyzed
   */
  std::vector<catalog::col_oid_t> GetColumnOids() const { return column_oids_; }

  /**
   * @return the alter table commands
   */
  std::vector<common::ManagedPointer<AlterCmdBase>> GetCommands() {
    std::vector<common::ManagedPointer<AlterCmdBase>> ret;
    ret.reserve(cmds_.size());

    for (const auto &cmd : cmds_) ret.emplace_back(common::ManagedPointer<AlterCmdBase>(cmd));
    return ret;
  }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the target table
   */
  catalog::table_oid_t table_oid_;

  /**
   * OIDs of the columns to be analyzed
   */
  std::vector<catalog::col_oid_t> column_oids_;

  /**
   * Commands
   */
  std::vector<std::unique_ptr<AlterCmdBase>> cmds_;
};

}  // namespace terrier::planner
