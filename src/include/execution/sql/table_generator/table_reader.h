#pragma once
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/sql/table_generator/schema_reader.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

// Forward declaration
namespace csv {
class CSVField;
}

namespace tpl::sql {
/**
 * This class reads table from files
 */
class TableReader {
 public:
  /**
   * Constructor
   * @param exec_ctx execution context to use
   */
  explicit TableReader(exec::ExecutionContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /**
   * Read a table given a schema file and a data file
   * @param schema_file file containing the schema
   * @param data_file csv file containing the data
   * @return
   */
  uint32_t ReadTable(const std::string &schema_file, const std::string &data_file);

 private:
  // Calls the accessor's create table
  terrier::catalog::SqlTableHelper *CreateTable(TableInfo *info);

  // Calls the accessor's create index
  std::vector<terrier::catalog::CatalogIndex *> CreateIndexes(TableInfo *info,
                                                              terrier::catalog::SqlTableHelper *catalog_table);

  // Writes a column according to its type.
  void WriteTableCol(terrier::storage::ProjectedRow *insert_pr, uint16_t col_offset, terrier::type::TypeId type,
                     csv::CSVField *field);

 private:
  // Postgres NULL string
  static constexpr const char *null_string = "\\N";
  exec::ExecutionContext *exec_ctx_;
};
}  // namespace tpl::sql
