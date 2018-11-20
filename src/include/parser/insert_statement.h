#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/sql_node_visitor.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * Represents the sql "INSERT ..."
 */
class InsertStatement : public SQLStatement {
 public:
  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<SelectStatement> select)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::SELECT),
        columns_(std::move(columns)),
        table_ref_(std::move(table_ref)),
        select_(std::move(select)) {}

  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<std::vector<std::vector<std::unique_ptr<AbstractExpression>>>> insert_values)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::VALUES),
        columns_(std::move(columns)),
        table_ref_(std::move(table_ref)),
        insert_values_(std::move(insert_values)) {}

  explicit InsertStatement(InsertType type) : SQLStatement(StatementType::INSERT), type_(type) {}

  ~InsertStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  std::string GetTableName() const { return table_ref_->GetTableName(); }

  const InsertType type_;
  const std::unique_ptr<std::vector<std::string>> columns_;
  const std::unique_ptr<TableRef> table_ref_;
  const std::unique_ptr<SelectStatement> select_;
  // TODO(WAN): unsure about this one.
  const std::unique_ptr<std::vector<std::vector<std::unique_ptr<AbstractExpression>>>> insert_values_;
};

}  // namespace parser
}  // namespace terrier
