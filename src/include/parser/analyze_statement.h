#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
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
  AnalyzeStatement(common::ManagedPointer<TableRef> analyze_table, std::vector<std::string> analyze_columns)
      : SQLStatement(StatementType::ANALYZE),
        analyze_table_(analyze_table),
        analyze_columns_(std::move(analyze_columns)) {}

  ~AnalyzeStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return analyze table
   */
  common::ManagedPointer<TableRef> GetAnalyzeTable() { return analyze_table_; }

  /**
   * @return analyze columns
   */
  std::vector<std::string> GetAnalyzeColumns() { return analyze_columns_; }

 private:
  const common::ManagedPointer<TableRef> analyze_table_;
  const std::vector<std::string> analyze_columns_;
};

}  // namespace parser
}  // namespace terrier
