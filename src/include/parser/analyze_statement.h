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

/**
 * Represents the sql "ANALYZE ...".
 */
class AnalyzeStatement : public SQLStatement {
 public:
  /**
   * Creates a new AnalyzeStatement.
   * @param analyze_table table to be analyzed
   * @param analyze_columns columns to be analyzed
   */
  AnalyzeStatement(std::shared_ptr<TableRef> analyze_table, std::vector<std::string> analyze_columns)
      : SQLStatement(StatementType::ANALYZE),
        analyze_table_(std::move(analyze_table)),
        analyze_columns_(std::move(analyze_columns)) {}

  ~AnalyzeStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return analyze table
   */
  std::shared_ptr<TableRef> GetAnalyzeTable() { return analyze_table_; }

  /**
   * @return analyze columns
   */
  std::vector<std::string> GetAnalyzeColumns() { return analyze_columns_; }

 private:
  // TODO(Gus): Remove shared pointer
  const std::shared_ptr<TableRef> analyze_table_;
  const std::vector<std::string> analyze_columns_;
};

}  // namespace parser
}  // namespace terrier
