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
  JoinDefinition(JoinType type, std::shared_ptr<TableRef> left, std::shared_ptr<TableRef> right,
                 std::shared_ptr<AbstractExpression> condition)
      : type_(type), left_(std::move(left)), right_(std::move(right)), condition_(std::move(condition)) {}

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
  std::shared_ptr<TableRef> GetLeftTable() { return left_; }

  /**
   * @return right table
   */
  std::shared_ptr<TableRef> GetRightTable() { return right_; }

  /**
   * @return join condition
   */
  std::shared_ptr<AbstractExpression> GetJoinCondition() { return condition_; }

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
  std::shared_ptr<TableRef> left_;
  std::shared_ptr<TableRef> right_;
  std::shared_ptr<AbstractExpression> condition_;
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
  TableRef(std::string alias, std::shared_ptr<TableInfo> table_info)
      : type_(TableReferenceType::NAME), alias_(std::move(alias)), table_info_(std::move(table_info)) {}

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   */
  TableRef(std::string alias, std::shared_ptr<SelectStatement> select)
      : type_(TableReferenceType::SELECT), alias_(std::move(alias)), select_(std::move(select)) {}

  /**
   * @param list table refs to use in creation
   */
  explicit TableRef(std::vector<std::shared_ptr<TableRef>> list)
      : type_(TableReferenceType::CROSS_PRODUCT), alias_(""), list_(std::move(list)) {}

  /**
   * @param join join definition to use in creation
   */
  explicit TableRef(std::shared_ptr<JoinDefinition> join)
      : type_(TableReferenceType::JOIN), alias_(""), join_(std::move(join)) {}

  /**
   * @param alias alias for table ref
   * @param table_info table info to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByName(std::string alias, std::shared_ptr<TableInfo> table_info) {
    return std::make_unique<TableRef>(std::move(alias), std::move(table_info));
  }

  /**
   * @param alias alias for table ref
   * @param select select statement to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefBySelect(std::string alias, std::shared_ptr<SelectStatement> select) {
    return std::make_unique<TableRef>(std::move(alias), std::move(select));
  }

  /**
   * @param list table refs to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByList(std::vector<std::shared_ptr<TableRef>> list) {
    return std::make_unique<TableRef>(std::move(list));
  }

  /**
   * @param join join definition to use in creation
   * @return unique pointer to the created table ref
   */
  static std::unique_ptr<TableRef> CreateTableRefByJoin(std::shared_ptr<JoinDefinition> join) {
    return std::make_unique<TableRef>(std::move(join));
  }

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
  std::shared_ptr<SelectStatement> GetSelect() { return select_; }

  /**
   * @return list of table references
   */
  std::vector<std::shared_ptr<TableRef>> GetList() { return list_; }

  /**
   * @return join
   */
  std::shared_ptr<JoinDefinition> GetJoin() { return join_; }

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
  std::shared_ptr<TableInfo> table_info_;

  std::shared_ptr<SelectStatement> select_;

  std::vector<std::shared_ptr<TableRef>> list_;

  std::shared_ptr<JoinDefinition> join_;
};

DEFINE_JSON_DECLARATIONS(TableRef);

}  // namespace parser
}  // namespace terrier
