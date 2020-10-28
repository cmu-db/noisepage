#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "common/json_header.h"
#include "expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"

namespace noisepage {
namespace binder {
class BindNodeVisitor;
}
namespace parser {
class SelectStatement;
class TableRef;

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

  /**
   * @return a copy of the join definition
   */
  std::unique_ptr<JoinDefinition> Copy();

  // TODO(WAN): not a SQLStatement?
  /**
   * @param v Visitor pattern for the statement
   */
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) { v->Visit(common::ManagedPointer(this)); }

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
  common::ManagedPointer<AbstractExpression> GetJoinCondition() { return condition_; }

  /**
   * @return the hashed value of this join definition
   */
  common::hash_t Hash() const;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two JoinDefinition are logically equal
   */
  bool operator==(const JoinDefinition &rhs) const;

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two JoinDefinition are logically unequal
   */
  bool operator!=(const JoinDefinition &rhs) const { return !(operator==(rhs)); }

  /**
   * @return JoinDefinition serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  JoinType type_;
  std::unique_ptr<TableRef> left_;
  std::unique_ptr<TableRef> right_;
  common::ManagedPointer<AbstractExpression> condition_;
};

DEFINE_JSON_HEADER_DECLARATIONS(JoinDefinition);

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
   * @return a copy of the table reference
   */
  std::unique_ptr<TableRef> Copy();

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

  /**
   * @param v Visitor pattern for the statement
   */
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) { v->Visit(common::ManagedPointer(this)); }

  /** @return table reference type*/
  TableReferenceType GetTableReferenceType() { return type_; }

  /** @return alias */
  std::string GetAlias() {
    if (alias_.empty()) alias_ = table_info_->GetTableName();
    return alias_;
  }

  /** @return table name */
  const std::string &GetTableName() { return table_info_->GetTableName(); }

  /** @return namespace name */
  const std::string &GetNamespaceName() { return table_info_->GetNamespaceName(); }

  /** @return database name */
  const std::string &GetDatabaseName() { return table_info_->GetDatabaseName(); }

  /** @return select statement */
  common::ManagedPointer<SelectStatement> GetSelect() { return common::ManagedPointer(select_); }

  /** @return list of table references */
  std::vector<common::ManagedPointer<TableRef>> GetList() {
    std::vector<common::ManagedPointer<TableRef>> list;
    list.reserve(list_.size());
    for (const auto &item : list_) {
      list.emplace_back(common::ManagedPointer(item));
    }
    return list;
  }

  /** @return join */
  common::ManagedPointer<JoinDefinition> GetJoin() { return common::ManagedPointer(join_); }

  /**
   * @return the hashed value of this table ref object
   */
  common::hash_t Hash() const;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two TableRef are logically equal
   */
  bool operator==(const TableRef &rhs) const;

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two TabelRef are logically unequal
   */
  bool operator!=(const TableRef &rhs) const { return !(operator==(rhs)); }

  /** @return TableRef serialized to json */
  nlohmann::json ToJson() const;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  friend class binder::BindNodeVisitor;

  TableReferenceType type_;
  std::string alias_;

  std::unique_ptr<TableInfo> table_info_;
  std::unique_ptr<SelectStatement> select_;

  std::vector<std::unique_ptr<TableRef>> list_;
  std::unique_ptr<JoinDefinition> join_;
};

DEFINE_JSON_HEADER_DECLARATIONS(TableRef);

}  // namespace parser
}  // namespace noisepage
