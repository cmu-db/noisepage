#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/managed_pointer.h"
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"

namespace terrier {
namespace parser {

class SelectStatement;

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
  JoinDefinition(JoinType type, common::ManagedPointer<TableRef> left, common::ManagedPointer<TableRef> right,
                 common::ManagedPointer<AbstractExpression> condition)
      : type_(type), left_(left), right_(right), condition_(condition) {}

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
  common::ManagedPointer<TableRef> GetLeftTable() { return left_; }

  /**
   * @return right table
   */
  common::ManagedPointer<TableRef> GetRightTable() { return right_; }

  /**
   * @return join condition
   */
  common::ManagedPointer<AbstractExpression> GetJoinCondition() { return condition_; }

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
  common::ManagedPointer<TableRef> left_;
  common::ManagedPointer<TableRef> right_;
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
  TableRef(std::string alias, common::ManagedPointer<TableInfo> table_info)
      : type_(TableReferenceType::NAME), alias_(std::move(alias)), table_info_(table_info) {}

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   */
  TableRef(std::string alias, common::ManagedPointer<SelectStatement> select)
      : type_(TableReferenceType::SELECT), alias_(std::move(alias)), select_(select) {}

  /**
   * @param list table refs to use in creation
   */
  explicit TableRef(std::vector<common::ManagedPointer<TableRef>> list)
      : type_(TableReferenceType::CROSS_PRODUCT), alias_(""), list_(std::move(list)) {}

  /**
   * @param join join definition to use in creation
   */
  explicit TableRef(common::ManagedPointer<JoinDefinition> join)
      : type_(TableReferenceType::JOIN), alias_(""), join_(join) {}

  /**
   * @param alias alias for table ref
   * @param table_info table info to use in creation
   * @return pointer to the created table ref
   */
  static TableRef *CreateTableRefByName(std::string alias, common::ManagedPointer<TableInfo> table_info) {
    return new TableRef(alias, table_info);
  }

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   * @return pointer to the created table ref
   */
  static TableRef *CreateTableRefBySelect(std::string alias, common::ManagedPointer<SelectStatement> select) {
    return new TableRef(alias, select);
  }

  /**
   * @param list table refs to use in creation
   * @return pointer to the created table ref
   */
  static TableRef *CreateTableRefByList(std::vector<common::ManagedPointer<TableRef>> list) {
    return new TableRef(std::move(list));
  }

  /**
   * @param join join definition to use in creation
   * @return pointer to the created table ref
   */
  static TableRef *CreateTableRefByJoin(common::ManagedPointer<JoinDefinition> join) { return new TableRef(join); }

  /**
   * @param v visitor
   */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /**
   * @return table reference type
   */
  TableReferenceType GetTableReferenceType() { return type_; }

  /**
   * @return alias
   */
  std::string GetAlias() { return alias_; }

  /**
   * @return table name
   */
  std::string GetTableName() { return table_info_->GetTableName(); }

  /**
   * @return schema name
   */
  std::string GetSchemaName() { return table_info_->GetSchemaName(); }

  /**
   * @return database name
   */
  std::string GetDatabaseName() { return table_info_->GetDatabaseName(); }

  /**
   * @return select statement
   */
  common::ManagedPointer<SelectStatement> GetSelect() { return select_; }

  /**
   * @return list of table references
   */
  std::vector<common::ManagedPointer<TableRef>> GetList() { return list_; }

  /**
   * @return join
   */
  common::ManagedPointer<JoinDefinition> GetJoin() { return join_; }

  /**
   * @return TableRef serialized to json
   */
  nlohmann::json ToJson() const;
  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j);

 private:
  TableReferenceType type_;
  std::string alias_;
  common::ManagedPointer<TableInfo> table_info_;

  common::ManagedPointer<SelectStatement> select_;

  std::vector<common::ManagedPointer<TableRef>> list_;

  common::ManagedPointer<JoinDefinition> join_;
};

DEFINE_JSON_DECLARATIONS(TableRef);

}  // namespace parser
}  // namespace terrier
