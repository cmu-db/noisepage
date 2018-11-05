#pragma once

#include <vector>

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

class SelectStatement;

// Definition of a join table
class JoinDefinition {
 public:
  JoinDefinition(JoinType type, std::unique_ptr<TableRef> left, std::unique_ptr<TableRef> right,
                 std::unique_ptr<AbstractExpression> condition)
      : type_(type), left_(std::move(left)), right_(std::move(right)), condition_(std::move(condition)) {}

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  const JoinType type_;
  const std::unique_ptr<TableRef> left_;
  const std::unique_ptr<TableRef> right_;
  const std::unique_ptr<AbstractExpression> condition_;
};

/**
 * Holds references to tables, either via table names or a select statement.
 */
struct TableRef {
 public:
  // TODO(WAN): was and is still a mess

  TableRef(std::string alias, std::unique_ptr<TableInfo> table_info)
      : type_(TableReferenceType::NAME), alias_(std::move(alias)), table_info_(std::move(table_info)) {}

  TableRef(std::string alias, std::unique_ptr<SelectStatement> select)
      : type_(TableReferenceType::SELECT), alias_(std::move(alias)), select_(std::move(select)) {}

  explicit TableRef(std::vector<std::unique_ptr<TableRef>> list)
      : type_(TableReferenceType::CROSS_PRODUCT), alias_(""), list_(std::move(list)) {}

  explicit TableRef(std::unique_ptr<JoinDefinition> join)
      : type_(TableReferenceType::JOIN), alias_(""), join_(std::move(join)) {}

  static std::unique_ptr<TableRef> CreateTableRefByName(std::string alias, std::unique_ptr<TableInfo> table_info) {
    return std::make_unique<TableRef>(alias, std::move(table_info));
  }

  static std::unique_ptr<TableRef> CreateTableRefBySelect(std::string alias, std::unique_ptr<SelectStatement> select) {
    return std::make_unique<TableRef>(alias, std::move(select));
  }

  static std::unique_ptr<TableRef> CreateTableRefByList(std::vector<std::unique_ptr<TableRef>> list) {
    return std::make_unique<TableRef>(std::move(list));
  }

  static std::unique_ptr<TableRef> CreateTableRefByJoin(std::unique_ptr<JoinDefinition> join) {
    return std::make_unique<TableRef>(std::move(join));
  }

  const TableReferenceType type_;
  const std::string alias_;
  const std::unique_ptr<TableInfo> table_info_;

  const std::unique_ptr<SelectStatement> select_;

  const std::vector<std::unique_ptr<TableRef>> list_;

  const std::unique_ptr<JoinDefinition> join_;

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }
};

}  // namespace parser
}  // namespace terrier
