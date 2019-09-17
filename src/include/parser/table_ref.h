#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/json.h"
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"

namespace terrier {
namespace binder {
class BindNodeVisitor;
}
namespace parser {

/**
 * Represents a join table.
 */
class JoinDefinition {
 public:
  /**
   * @param type join type
   * @param left left table
   * @param right right table
   * @param condition join condition
   */
  JoinDefinition(JoinType type, std::unique_ptr<TableRef> left, std::unique_ptr<TableRef> right,
                 common::ManagedPointer<AbstractExpression> condition)
      : type_(type), left_(std::move(left)), right_(std::move(right)), condition_(condition) {}

  /**
   * Default constructor used for deserialization
   */
  JoinDefinition() = default;

  // TODO(WAN): not a SQLStatement?
  /**
   * @param v visitor
   */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /**
   * @return type of join
   */
  JoinType GetJoinType() { return type_; }

  /**
   * @return left table
   */
  common::ManagedPointer<TableRef> GetLeftTable() { return common::ManagedPointer(left_); }

  /**
   * @return right table
   */
  common::ManagedPointer<TableRef> GetRightTable() { return common::ManagedPointer(right_); }

  /**
   * @return join condition
   */
  common::ManagedPointer<AbstractExpression> GetJoinCondition() { return common::ManagedPointer(condition_); }

  /**
   * @return JoinDefinition serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j);

 private:
  JoinType type_;
  std::unique_ptr<TableRef> left_;
  std::unique_ptr<TableRef> right_;
  common::ManagedPointer<AbstractExpression> condition_;
};

DEFINE_JSON_DECLARATIONS(JoinDefinition);

/**
 * Holds references to tables, either via table names or a select statement.
 */
class TableRef {
 public:
  // TODO(WAN): was and is still a mess

  /**
   * Default constructor used for deserialization
   */
  TableRef() = default;

  /**
   * @param alias alias for table ref
   * @param table_info table information to use in creation
   */
  TableRef(std::string alias, std::unique_ptr<TableInfo> table_info)
      : type_(TableReferenceType::NAME), alias_(std::move(alias)), table_info_(std::move(table_info)) {}

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   */
  TableRef(std::string alias, std::unique_ptr<SelectStatement> select)
      : type_(TableReferenceType::SELECT), alias_(std::move(alias)), select_(std::move(select)) {}

  /**
   * @param list table refs to use in creation
   */
  explicit TableRef(std::vector<std::unique_ptr<TableRef>> list)
      : type_(TableReferenceType::CROSS_PRODUCT), alias_(""), list_(std::move(list)) {}

  /**
   * @param join join definition to use in creation
   */
  explicit TableRef(std::unique_ptr<JoinDefinition> join)
      : type_(TableReferenceType::JOIN), alias_(""), join_(std::move(join)) {}

  /**
   * @param alias alias for table ref
   * @param table_info table info to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByName(std::string alias, std::unique_ptr<TableInfo> table_info) {
    return std::make_unique<TableRef>(std::move(alias), std::move(table_info));
  }

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefBySelect(std::string alias, std::unique_ptr<SelectStatement> select) {
    return std::make_unique<TableRef>(std::move(alias), std::move(select));
  }

  /**
   * @param list table refs to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByList(std::vector<std::unique_ptr<TableRef>> list) {
    return std::make_unique<TableRef>(std::move(list));
  }

  /**
   * @param join join definition to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByJoin(std::unique_ptr<JoinDefinition> join) {
    return std::make_unique<TableRef>(std::move(join));
  }

  /** @param v visitor */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /** @return table reference type*/
  TableReferenceType GetTableReferenceType() { return type_; }

  /** @return alias */
  const std::string GetAlias() {
    if (alias_.empty()) alias_ = table_info_->GetTableName();
    return alias_;
  }

  /** @return table name */
  std::string GetTableName() { return table_info_->GetTableName(); }

  /** @return schema name */
  std::string GetSchemaName() { return table_info_->GetSchemaName(); }

  /** @return database name */
  std::string GetDatabaseName() { return table_info_->GetDatabaseName(); }

  /** @return select statement */
  common::ManagedPointer<SelectStatement> GetSelect() { return common::ManagedPointer(select_); }

  /** @return list of table references */
  const std::vector<std::unique_ptr<TableRef>> &GetList() { return list_; }

  /** @return join */
  common::ManagedPointer<JoinDefinition> GetJoin() { return common::ManagedPointer(join_); }

  /** @return TableRef serialized to json */
  nlohmann::json ToJson() const;

  /** @param j json to deserialize */
  void FromJson(const nlohmann::json &j);

 private:
  friend class binder::BindNodeVisitor;

  TableReferenceType type_;
  std::string alias_;

  std::unique_ptr<TableInfo> table_info_;
  std::unique_ptr<SelectStatement> select_;

  std::vector<std::unique_ptr<TableRef>> list_;
  std::unique_ptr<JoinDefinition> join_;

  /**
   * Check if the current table ref has the correct database name.
   * If the current table ref does not have a database name, set the database name to the default database name
   * If the current table ref has a database name, this function verifies if it matches the defualt database name
   * @param default_database_name Default database name
   */
  void TryBindDatabaseName(const std::string &default_database_name) {
    if (!table_info_) table_info_ = std::make_unique<TableInfo>();
    table_info_->TryBindDatabaseName(default_database_name);
  }
};

DEFINE_JSON_DECLARATIONS(TableRef);

}  // namespace parser
}  // namespace terrier
