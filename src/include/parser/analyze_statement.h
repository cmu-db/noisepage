#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {

class AnalyzeStatement : public SQLStatement {
 public:
  AnalyzeStatement(std::unique_ptr<TableRef> analyze_table, std::unique_ptr<std::vector<std::string>> analyze_columns)
      : SQLStatement(StatementType::ANALYZE),
        analyze_table_(std::move(analyze_table)),
        analyze_columns_(std::move(analyze_columns)) {}

  ~AnalyzeStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::unique_ptr<TableRef> analyze_table_;
  const std::unique_ptr<std::vector<std::string>> analyze_columns_;
};

}  // namespace parser
}  // namespace terrier
