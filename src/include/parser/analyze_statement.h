#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "common/managed_pointer.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace noisepage {
namespace parser {
/**
 * AnalyzeStatement represents the sql "ANALYZE ...".
 */
class AnalyzeStatement : public SQLStatement {
 public:
  /**
   * Creates a new AnalyzeStatement.
   * @param analyze_table table to be analyzed
   * @param analyze_columns columns to be analyzed
   */
  AnalyzeStatement(std::unique_ptr<TableRef> analyze_table, std::unique_ptr<std::vector<std::string>> analyze_columns)
      : SQLStatement(StatementType::ANALYZE),
        analyze_table_(std::move(analyze_table)),
        analyze_columns_(std::move(analyze_columns)) {}

  ~AnalyzeStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return analyze table */
  common::ManagedPointer<TableRef> GetAnalyzeTable() { return common::ManagedPointer(analyze_table_); }

  /** @return analyze columns */
  common::ManagedPointer<std::vector<std::string>> GetColumns() { return common::ManagedPointer(analyze_columns_); }

  catalog::db_oid_t GetDatabaseOid() { return database_oid_; }

  void SetDatabaseOid(catalog::db_oid_t database_oid) { database_oid_ = database_oid; }

  catalog::table_oid_t GetTableOid() { return table_oid_; }

  void SetTableOid(catalog::table_oid_t table_oid) { table_oid_ = table_oid; }

  const std::vector<catalog::col_oid_t> &GetColumnOids() { return column_oids_; }

  void AddColumnOid(catalog::col_oid_t col_oid) { column_oids_.push_back(col_oid); }

 private:
  std::unique_ptr<TableRef> analyze_table_;
  std::unique_ptr<std::vector<std::string>> analyze_columns_;
  catalog::db_oid_t database_oid_;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> column_oids_;
};

}  // namespace parser
}  // namespace noisepage
