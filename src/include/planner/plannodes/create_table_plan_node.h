#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/select_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

/**
 * Primary key
 */
struct PrimaryKeyInfo {
  /**
   * Columns of the primary key
   */
  std::vector<std::string> primary_key_cols_;
  /**
   * Name of this constraint
   */
  std::string constraint_name_;

  /**
   * @return serialized PrimaryKeyInfo
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["primary_key_cols"] = primary_key_cols_;
    j["constraint_name"] = constraint_name_;
    return j;
  }

  /**
   * Deserializes a PrimaryKeyInfo
   * @param j serialized json of PrimaryKeyInfo
   */
  void FromJson(const nlohmann::json &j) {
    primary_key_cols_ = j.at("primary_key_cols").get<std::vector<std::string>>();
    constraint_name_ = j.at("constraint_name").get<std::string>();
  }

  /**
   * @return the hashed value of this primary key info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash primary_key_cols
    hash = common::HashUtil::CombineHashInRange(hash, primary_key_cols_.begin(), primary_key_cols_.end());
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are logically equal
   */
  bool operator==(const PrimaryKeyInfo &rhs) const {
    if (constraint_name_ != rhs.constraint_name_) return false;

    if (primary_key_cols_.size() != rhs.primary_key_cols_.size()) return false;
    for (size_t i = 0; i < primary_key_cols_.size(); i++) {
      if (primary_key_cols_[i] != rhs.primary_key_cols_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const PrimaryKeyInfo &rhs) const { return !(*this == rhs); }
};

/**
 * Foreign key constraints
 */
struct ForeignKeyInfo {
  /**
   * Sources of foreign key constraints
   */
  std::vector<std::string> foreign_key_sources_;
  /**
   * Sinks of foreign key constraints
   */
  std::vector<std::string> foreign_key_sinks_;
  /**
   * Name of the sink table
   */
  std::string sink_table_name_;
  /**
   * Name of this constraint
   */
  std::string constraint_name_;
  /**
   * Update action
   */
  parser::FKConstrActionType upd_action_;
  /**
   * Delete action
   */
  parser::FKConstrActionType del_action_;

  /**
   * @return serialized ForeignKeyInfo
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["foreign_key_sources"] = foreign_key_sources_;
    j["foreign_key_sinks"] = foreign_key_sinks_;
    j["sink_table_name"] = sink_table_name_;
    j["constraint_name"] = constraint_name_;
    j["upd_action"] = upd_action_;
    j["del_action"] = del_action_;
    return j;
  }

  /**
   * Deserializes a ForeignKeyInfo
   * @param j serialized json of ForeignKeyInfo
   */
  void FromJson(const nlohmann::json &j) {
    foreign_key_sources_ = j.at("foreign_key_sources").get<std::vector<std::string>>();
    foreign_key_sinks_ = j.at("foreign_key_sinks").get<std::vector<std::string>>();
    sink_table_name_ = j.at("sink_table_name").get<std::string>();
    constraint_name_ = j.at("constraint_name").get<std::string>();
    upd_action_ = j.at("upd_action").get<parser::FKConstrActionType>();
    del_action_ = j.at("del_action").get<parser::FKConstrActionType>();
  }

  /**
   * @return the hashed value of this foreign key info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash sink_table_name
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sink_table_name_));

    // Hash foreign_key_sources
    hash = common::HashUtil::CombineHashInRange(hash, foreign_key_sources_.begin(), foreign_key_sources_.end());

    // Hash foreign_key_sinks
    hash = common::HashUtil::CombineHashInRange(hash, foreign_key_sinks_.begin(), foreign_key_sinks_.end());

    // Hash upd_action
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(upd_action_));

    // Hash del_action
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(del_action_));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two check info are logically equal
   */
  bool operator==(const ForeignKeyInfo &rhs) const {
    // Constraint Name
    if (constraint_name_ != rhs.constraint_name_) return false;

    // Sink Table Name
    if (sink_table_name_ != rhs.sink_table_name_) return false;

    // Update TransactionAction
    if (upd_action_ != rhs.upd_action_) return false;

    // Delete TransactionAction
    if (del_action_ != rhs.del_action_) return false;

    // Foreign Key Sources
    if (foreign_key_sources_ != rhs.foreign_key_sources_) return false;

    // Foreign Key Sinks
    if (foreign_key_sinks_ != rhs.foreign_key_sinks_) return false;

    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const ForeignKeyInfo &rhs) const { return !(*this == rhs); }
};

/**
 * Unique constraints
 */
struct UniqueInfo {
  /**
   * Columns that need to have unique values
   */
  std::vector<std::string> unique_cols_;
  /**
   * Name of this constraint
   */
  std::string constraint_name_;

  /**
   * @return serialized UniqueInfo
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["unique_cols"] = unique_cols_;
    j["constraint_name"] = constraint_name_;
    return j;
  }

  /**
   * Deserializes a UniqueInfo
   * @param j serialized json of UniqueInfo
   */
  void FromJson(const nlohmann::json &j) {
    unique_cols_ = j.at("unique_cols").get<std::vector<std::string>>();
    constraint_name_ = j.at("constraint_name").get<std::string>();
  }

  /**
   * @return the hashed value of this unique info
   */
  common::hash_t Hash() const {
    // Constraint Name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Unique Columns
    hash = common::HashUtil::CombineHashInRange(hash, unique_cols_.begin(), unique_cols_.end());

    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two unique info are logically equal
   */
  bool operator==(const UniqueInfo &rhs) const {
    // Constraint Name
    if (constraint_name_ != rhs.constraint_name_) return false;

    // Unique Columns
    if (unique_cols_ != rhs.unique_cols_) return false;

    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const UniqueInfo &rhs) const { return !(*this == rhs); }
};

/**
 * Check constraints
 */
struct CheckInfo {
  /**
   * Columns that need to be checked
   */
  std::vector<std::string> check_cols_;
  /**
   * Name of this constraint
   */
  std::string constraint_name_;
  /**
   * Type of expression to be checked
   */
  parser::ExpressionType expr_type_;
  /**
   * Value of expression to be checked
   */
  type::TransientValue expr_value_;

  /**
   * @return serialized CheckInfo
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["check_cols"] = check_cols_;
    j["constraint_name"] = constraint_name_;
    j["expr_type"] = expr_type_;
    j["expr_value"] = expr_value_;
    return j;
  }

  /**
   * Deserializes a check info
   * @param j serialized json of check info
   */
  void FromJson(const nlohmann::json &j) {
    check_cols_ = j.at("check_cols").get<std::vector<std::string>>();
    constraint_name_ = j.at("constraint_name").get<std::string>();
    expr_type_ = j.at("expr_type").get<parser::ExpressionType>();
    expr_value_ = j.at("expr_value").get<type::TransientValue>();
  }

  /**
   * CheckInfo constructor
   * @param check_cols name of the columns to be checked
   * @param constraint_name name of the constraint
   * @param expr_type the type of the expression to be satisfied
   * @param expr_value the value of the expression to be satisfied
   */
  CheckInfo(std::vector<std::string> check_cols, std::string constraint_name, parser::ExpressionType expr_type,
            type::TransientValue expr_value)
      : check_cols_(std::move(check_cols)),
        constraint_name_(std::move(constraint_name)),
        expr_type_(expr_type),
        expr_value_(std::move(expr_value)) {}

  /**
   * Default constructor for deserialization
   */
  CheckInfo() = default;

  /**
   * @return the hashed value of this check info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash check_cols
    hash = common::HashUtil::CombineHashInRange(hash, check_cols_.begin(), check_cols_.end());

    // Hash expr_type
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(expr_type_));

    // Hash expr_value
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(expr_value_.Hash()));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two check info are logically equal
   */
  bool operator==(const CheckInfo &rhs) const {
    // Constraint Name
    if (constraint_name_ != rhs.constraint_name_) return false;

    // Expression Type
    if (expr_type_ != rhs.expr_type_) return false;

    // Expression Value
    if (expr_value_ != rhs.expr_value_) return false;

    // Check Columns
    if (check_cols_ != rhs.check_cols_) return false;

    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const CheckInfo &rhs) const { return !(*this == rhs); }
};

/**
 * Plan node for creating tables
 */
class CreateTablePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create table plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid  OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * @param table_name the name of the table
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param table_schema the schema of the table
     * @return builder object
     */
    Builder &SetTableSchema(std::shared_ptr<catalog::Schema> table_schema) {
      table_schema_ = std::move(table_schema);
      return *this;
    }

    /**
     * @param has_primary_key has_primary_key true if index/table has primary key
     * @return builder object
     */
    Builder &SetHasPrimaryKey(bool has_primary_key) {
      has_primary_key_ = has_primary_key;
      return *this;
    }

    /**
     * @param primary_key primary_key of table
     * @return builder object
     */
    Builder &SetPrimaryKey(PrimaryKeyInfo primary_key) {
      primary_key_ = std::move(primary_key);
      return *this;
    }

    /**
     * @param foreign_keys foreign keys meta-data
     * @return builder object
     */
    Builder &SetForeignKeys(std::vector<ForeignKeyInfo> &&foreign_keys) {
      foreign_keys_ = foreign_keys;
      return *this;
    }

    /**
     * @param con_uniques unique constraints
     * @return builder object
     */
    Builder &SetUniqueConstraints(std::vector<UniqueInfo> &&con_uniques) {
      con_uniques_ = std::move(con_uniques);
      return *this;
    }

    /**
     * @param con_checks check constraints
     * @return builder object
     */
    Builder &SetCheckConstraints(std::vector<CheckInfo> &&con_checks) {
      con_checks_ = std::move(con_checks);
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kTable) {
        table_name_ = std::string(create_stmt->GetTableName());
        std::vector<catalog::Schema::Column> columns;
        std::vector<std::string> pri_cols;

        for (auto &col : create_stmt->GetColumns()) {
          type::TypeId val = col->GetValueType();

          // Create column
          // TODO(John) The default value expressions in the column definitions are currently shared pointers.
          // The dereferences below are completely unsafe (there is no safe way to do it), but not fatal at the
          // moment because we don't actually use them yet... Fixing this requires overhauling the plannodes to strip
          // away shared pointers.
          if (col->GetVarlenSize() != 0) {
            TERRIER_ASSERT(val == type::TypeId::VARCHAR || val == type::TypeId::VARBINARY,
                           "Variable length types should have a non-zero max varlen size");
            columns.emplace_back(std::string(col->GetColumnName()), val, col->GetVarlenSize(), false,
                                 *col->GetDefaultExpression());
          } else {
            TERRIER_ASSERT(val != type::TypeId::VARCHAR && val != type::TypeId::VARBINARY,
                           "Fixed length types should have max varlen of size 0");
            columns.emplace_back(std::string(col->GetColumnName()), val, false, *col->GetDefaultExpression());
          }

          // Collect Multi-column constraints information

          // Primary key
          if (col->IsPrimaryKey()) {
            pri_cols.push_back(col->GetColumnName());
          }

          // Unique constraint
          // Currently only supports for single column
          if (col->IsUnique()) {
            ProcessUniqueConstraint(col);
          }

          // Check expression constraint
          // Currently only supports simple boolean forms like (a > 0)
          if (col->GetCheckExpression() != nullptr) {
            ProcessCheckConstraint(col);
          }
        }

        // The parser puts the multi-column constraint information
        // into an artificial ColumnDefinition.
        // primary key constraint
        if (!pri_cols.empty()) {
          primary_key_.primary_key_cols_ = pri_cols;
          primary_key_.constraint_name_ = "con_primary";
          has_primary_key_ = true;
        }

        // foreign key
        for (auto &fk : create_stmt->GetForeignKeys()) {
          ProcessForeignKeyConstraint(table_name_, fk);
        }

        table_schema_ = std::make_shared<catalog::Schema>(columns);
      }
      return *this;
    }

    /**
     * Extract foreign key constraints from column definition
     * @param table_name name of the table to get foreign key constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessForeignKeyConstraint(const std::string &table_name,
                                         const std::shared_ptr<parser::ColumnDefinition> &col) {
      ForeignKeyInfo fkey_info;

      fkey_info.foreign_key_sources_ = std::vector<std::string>();
      fkey_info.foreign_key_sinks_ = std::vector<std::string>();

      // Extract source and sink column names
      for (auto &key : col->GetForeignKeySources()) {
        fkey_info.foreign_key_sources_.push_back(key);
      }
      for (auto &key : col->GetForeignKeySinks()) {
        fkey_info.foreign_key_sinks_.push_back(key);
      }

      // Extract table names
      fkey_info.sink_table_name_ = col->GetForeignKeySinkTableName();

      // Extract delete and update actions
      fkey_info.upd_action_ = col->GetForeignKeyUpdateAction();
      fkey_info.del_action_ = col->GetForeignKeyDeleteAction();

      fkey_info.constraint_name_ = "FK_" + table_name + "->" + fkey_info.sink_table_name_;

      foreign_keys_.push_back(fkey_info);
      return *this;
    }

    /**
     * Extract unique constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessUniqueConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
      UniqueInfo unique_info;

      unique_info.unique_cols_ = {col->GetColumnName()};
      unique_info.constraint_name_ = "con_unique";

      con_uniques_.push_back(unique_info);
      return *this;
    }

    /**
     * Extract check constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessCheckConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
      auto check_cols = std::vector<std::string>();

      // TODO(Gus,Wen) more expression types need to be supported
      if (col->GetCheckExpression()->GetReturnValueType() == type::TypeId::BOOLEAN) {
        check_cols.push_back(col->GetColumnName());

        parser::ConstantValueExpression *const_expr_elem =
            dynamic_cast<parser::ConstantValueExpression *>(col->GetCheckExpression()->GetChild(1).get());
        type::TransientValue tmp_value = const_expr_elem->GetValue();

        CheckInfo check_info(check_cols, "con_check", col->GetCheckExpression()->GetExpressionType(),
                             std::move(tmp_value));
        con_checks_.emplace_back(std::move(check_info));
      }
      return *this;
    }

    /**
     * Build the create table plan node
     * @return plan node
     */
    std::shared_ptr<CreateTablePlanNode> Build() {
      return std::shared_ptr<CreateTablePlanNode>(new CreateTablePlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, std::move(table_name_),
          std::move(table_schema_), has_primary_key_, std::move(primary_key_), std::move(foreign_keys_),
          std::move(con_uniques_), std::move(con_checks_)));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of the schema/namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * Table Name
     */
    std::string table_name_;

    /**
     * Table Schema
     */
    std::shared_ptr<catalog::Schema> table_schema_;

    /**
     * ColumnDefinition for multi-column constraints (including foreign key)
     * Whether the table/index has primary key
     */
    bool has_primary_key_ = false;

    /**
     * Primary key information
     */
    PrimaryKeyInfo primary_key_;

    /**
     * Foreign keys information
     */
    std::vector<ForeignKeyInfo> foreign_keys_;

    /**
     * Unique constraints
     */
    std::vector<UniqueInfo> con_uniques_;

    /**
     * Check constraints
     */
    std::vector<CheckInfo> con_checks_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_name the name of the table
   * @param table_schema schema of the table to create
   * @param has_primary_key true if index/table has primary key
   * @param primary_key primary_key of table
   * @param foreign_keys foreign keys meta-data
   * @param con_uniques unique constraints
   * @param con_checks check constraints
   */
  CreateTablePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                      std::shared_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                      catalog::namespace_oid_t namespace_oid, std::string table_name,
                      std::shared_ptr<catalog::Schema> table_schema, bool has_primary_key, PrimaryKeyInfo primary_key,
                      std::vector<ForeignKeyInfo> &&foreign_keys, std::vector<UniqueInfo> &&con_uniques,
                      std::vector<CheckInfo> &&con_checks)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_name_(std::move(table_name)),
        table_schema_(std::move(table_schema)),
        has_primary_key_(has_primary_key),
        primary_key_(std::move(primary_key)),
        foreign_keys_(std::move(foreign_keys)),
        con_uniques_(std::move(con_uniques)),
        con_checks_(std::move(con_checks)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  CreateTablePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateTablePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_TABLE; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace to create index on
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return name of the table
   */
  const std::string &GetTableName() const { return table_name_; }

  /**
   * @return pointer to the schema
   */
  std::shared_ptr<catalog::Schema> GetTableSchema() const { return table_schema_; }

  /**
   * @return true if index/table has primary key
   */
  bool HasPrimaryKey() const { return has_primary_key_; }

  /**
   * @return primary key meta-data
   */
  PrimaryKeyInfo GetPrimaryKey() const { return primary_key_; }

  /**
   * @return foreign keys meta-data
   */
  const std::vector<ForeignKeyInfo> &GetForeignKeys() const { return foreign_keys_; }

  /**
   * @return unique constraints
   */
  const std::vector<UniqueInfo> &GetUniqueConstraints() const { return con_uniques_; }

  /**
   * @return check constraints
   */
  const std::vector<CheckInfo> &GetCheckConstraints() const { return con_checks_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Table Name
   */
  std::string table_name_;

  /**
   * Table Schema
   */
  std::shared_ptr<catalog::Schema> table_schema_;

  /**
   * ColumnDefinition for multi-column constraints (including foreign key)
   * Whether the table/index has primary key
   */
  bool has_primary_key_ = false;

  /**
   * Primary key information
   */
  PrimaryKeyInfo primary_key_;

  /**
   * Foreign keys information
   */
  std::vector<ForeignKeyInfo> foreign_keys_;

  /**
   * Unique constraints
   */
  std::vector<UniqueInfo> con_uniques_;

  /**
   * Check constraints
   */
  std::vector<CheckInfo> con_checks_;
};

DEFINE_JSON_DECLARATIONS(PrimaryKeyInfo);
DEFINE_JSON_DECLARATIONS(ForeignKeyInfo);
DEFINE_JSON_DECLARATIONS(UniqueInfo);
DEFINE_JSON_DECLARATIONS(CheckInfo);
DEFINE_JSON_DECLARATIONS(CreateTablePlanNode);

}  // namespace terrier::planner
