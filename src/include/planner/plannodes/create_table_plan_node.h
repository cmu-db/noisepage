#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/postgres/name_builder.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/select_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
  nlohmann::json ToJson() const;

  /**
   * Deserializes a PrimaryKeyInfo
   * @param j serialized json of PrimaryKeyInfo
   */
  void FromJson(const nlohmann::json &j);

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
  nlohmann::json ToJson() const;

  /**
   * Deserializes a ForeignKeyInfo
   * @param j serialized json of ForeignKeyInfo
   */
  void FromJson(const nlohmann::json &j);

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
  nlohmann::json ToJson() const;

  /**
   * Deserializes a UniqueInfo
   * @param j serialized json of UniqueInfo
   */
  void FromJson(const nlohmann::json &j);

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
  std::vector<std::string> check_cols_;  // TODO(Matt): always size 1 according to ProcessCheckConstraint?
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
  parser::ConstantValueExpression expr_value_;

  /**
   * @return serialized CheckInfo
   */
  nlohmann::json ToJson() const;

  /**
   * Deserializes a check info
   * @param j serialized json of check info
   */
  void FromJson(const nlohmann::json &j);

  /**
   * CheckInfo constructor
   * @param check_cols name of the columns to be checked
   * @param constraint_name name of the constraint
   * @param expr_type the type of the expression to be satisfied
   * @param expr_value the value of the expression to be satisfied
   */
  CheckInfo(std::vector<std::string> check_cols, std::string constraint_name, parser::ExpressionType expr_type,
            parser::ConstantValueExpression expr_value)
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
    Builder &SetTableSchema(std::unique_ptr<catalog::Schema> table_schema) {
      table_schema_ = std::move(table_schema);
      return *this;
    }

    /**
     * @param block_store the BlockStore to associate with this table
     * @return builder object
     */
    Builder &SetBlockStore(common::ManagedPointer<storage::BlockStore> block_store) {
      block_store_ = block_store;
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
     * Extract foreign key constraints from column definition
     * @param table_name name of the table to get foreign key constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessForeignKeyConstraint(const std::string &table_name,
                                         const common::ManagedPointer<parser::ColumnDefinition> col) {
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

      fkey_info.constraint_name_ = catalog::postgres::NameBuilder::MakeName(
          table_name, fkey_info.sink_table_name_, catalog::postgres::NameBuilder::FOREIGN_KEY);

      foreign_keys_.push_back(fkey_info);
      return *this;
    }

    /**
     * Extract unique constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessUniqueConstraint(const common::ManagedPointer<parser::ColumnDefinition> col) {
      UniqueInfo unique_info;

      unique_info.unique_cols_ = {col->GetColumnName()};
      unique_info.constraint_name_ = catalog::postgres::NameBuilder::MakeName(
          table_name_, col->GetColumnName(), catalog::postgres::NameBuilder::UNIQUE_KEY);

      con_uniques_.push_back(unique_info);
      return *this;
    }

    /**
     * Extract check constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessCheckConstraint(const common::ManagedPointer<parser::ColumnDefinition> col) {
      auto check_cols = std::vector<std::string>();

      // TODO(Gus,Wen) more expression types need to be supported
      if (col->GetCheckExpression()->GetReturnValueType() == type::TypeId::BOOLEAN) {
        check_cols.push_back(col->GetColumnName());

        common::ManagedPointer<parser::ConstantValueExpression> const_expr_elem =
            (col->GetCheckExpression()->GetChild(1)).CastManagedPointerTo<parser::ConstantValueExpression>();
        auto tmp_value = *const_expr_elem;

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
    std::unique_ptr<CreateTablePlanNode> Build();

   protected:
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
    std::unique_ptr<catalog::Schema> table_schema_;

    /**
     * block store to be used when constructing this table
     */
    common::ManagedPointer<storage::BlockStore> block_store_;

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
  CreateTablePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                      std::unique_ptr<OutputSchema> output_schema, catalog::namespace_oid_t namespace_oid,
                      std::string table_name, std::unique_ptr<catalog::Schema> table_schema,
                      common::ManagedPointer<storage::BlockStore> block_store, bool has_primary_key,
                      PrimaryKeyInfo primary_key, std::vector<ForeignKeyInfo> &&foreign_keys,
                      std::vector<UniqueInfo> &&con_uniques, std::vector<CheckInfo> &&con_checks);

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
   * @return OID of the namespace to create index on
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return name of the table
   */
  const std::string &GetTableName() const { return table_name_; }

  /**
   * @return block store to be used when constructing this table
   */
  common::ManagedPointer<storage::BlockStore> GetBlockStore() const { return block_store_; }

  /**
   * @return pointer to the schema
   */
  common::ManagedPointer<catalog::Schema> GetSchema() const { return common::ManagedPointer(table_schema_); }

  /**
   * @return true if index/table has primary key
   */
  bool HasPrimaryKey() const { return has_primary_key_; }

  /**
   * @return primary key meta-data
   */
  const PrimaryKeyInfo &GetPrimaryKey() const { return primary_key_; }

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

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
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
  std::unique_ptr<catalog::Schema> table_schema_;

  common::ManagedPointer<storage::BlockStore> block_store_;

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

DEFINE_JSON_HEADER_DECLARATIONS(PrimaryKeyInfo);
DEFINE_JSON_HEADER_DECLARATIONS(ForeignKeyInfo);
DEFINE_JSON_HEADER_DECLARATIONS(UniqueInfo);
DEFINE_JSON_HEADER_DECLARATIONS(CheckInfo);
DEFINE_JSON_HEADER_DECLARATIONS(CreateTablePlanNode);

}  // namespace noisepage::planner
