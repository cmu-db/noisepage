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
  AlterCmdBase() = default;

  AlterCmdBase(catalog::col_oid_t oid) : oid_(oid) {}

  virtual ~AlterCmdBase() = default;

  /**
   * @return the type of the command
   */
  virtual parser::AlterTableStatement::AlterType GetType() const = 0;

  /**
   * @return the oid of the column
   */
  const catalog::col_oid_t GetColOid() const { return oid_; }

  /**
   * @return  json encoded
   */
  virtual nlohmann::json ToJson() const = 0;

  /**
   * Deserialzies the command
   * @param j serielzied json of the AlterCmdBase
   */
  virtual void FromJson(const nlohmann::json &j) = 0;

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

    AddColumnCmd(catalog::Schema::Column col, std::unique_ptr<ForeignKeyInfo> foreign_key,
                 std::unique_ptr<UniqueInfo> con_unique, std::unique_ptr<CheckInfo> con_check)
        : AlterCmdBase(col.Oid()),
          col_(col),
          foreign_key_(std::move(foreign_key)),
          con_unique_(std::move(con_unique)),
          con_check_(std::move(con_check)) {}

    // TODO(SC)
    /**
     * Serializes a AddColumnCmd
     * @return
     */
    nlohmann::json ToJson() const override {
      nlohmann::json j;
      return j;
    }

    parser::AlterTableStatement::AlterType GetType() const override {
      return parser::AlterTableStatement::AlterType::AddColumn;
    }

    const catalog::Schema::Column &GetColumn() const { return col_; }

    // TODO(SC)
    /**
     * Deserialzies an AddColumnCmd
     */
    void FromJson(const nlohmann::json &j) override { (void)j; }

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
     * Unique Constrain
     */
    std::unique_ptr<UniqueInfo> con_unique_ = nullptr;

    /**
     * Check constrain
     */
    std::unique_ptr<CheckInfo> con_check_ = nullptr;
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

    Builder &SetCommands(std::vector<std::unique_ptr<AlterCmdBase>> &&cmds) {
      cmds_ = std::move(cmds);
      return *this;
    }

    /**
     * Build the analyze plan node
     * @return plan node
     */
    std::unique_ptr<AlterPlanNode> Build() {
      return std::unique_ptr<AlterPlanNode>(new AlterPlanNode(std::move(children_), std::move(output_schema_),
                                                              table_oid_, std::move(column_oids_), std::move(cmds_)));
    }

    /**
     * Extract unique constrain
     * @param col_def
     * @return uniqueinfo
     */
    std::unique_ptr<UniqueInfo> ProcessUniqueConstrain(const parser::ColumnDefinition &col_def) {
      // TODO(XC): after we are parsing this
      return nullptr;
    }

    /**
     * Extrack Check constrain
     * @param col_def
     * @return
     */
    std::unique_ptr<CheckInfo> ProcessCheckConstrain(const parser::ColumnDefinition &col_def) {
      // TODO(XC): after we are parsing this
      return nullptr;
    }

    /**
     * Extract foreign key info
     * @param col_def
     * @return
     */
    std::unique_ptr<ForeignKeyInfo> ProcessForeignKeyConstrain(const parser::ColumnDefinition &col_def) {
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

    for (const auto &cmd : cmds_) ret.push_back(common::ManagedPointer<AlterCmdBase>(cmd));
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
