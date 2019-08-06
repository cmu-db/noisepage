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

namespace terrier::sql {
/**
 * This class reads table from files
 */
class TableReader {
 public:
  /**
   * Constructor
   * @param exec_ctx execution context to use
   * @param store block store to use when creating tables
   * @param ns_oid oid of the namespace
   */
  explicit TableReader(exec::ExecutionContext *exec_ctx, terrier::storage::BlockStore *store,
                       terrier::catalog::namespace_oid_t ns_oid)
      : exec_ctx_{exec_ctx}, store_{store}, ns_oid_{ns_oid} {}

  /**
   * Read a table given a schema file and a data file
   * @param schema_file file containing the schema
   * @param data_file csv file containing the data
   * @return
   */
  uint32_t ReadTable(const std::string &schema_file, const std::string &data_file);

 private:
  // Calls the accessor's create table
  terrier::catalog::table_oid_t CreateTable(TableInfo *info);

  // Calls the accessor's create index
  std::vector<terrier::catalog::index_oid_t> CreateIndexes(TableInfo *info, terrier::catalog::table_oid_t table_oid);

  // Writes a column according to its type.
  void WriteTableCol(terrier::storage::ProjectedRow *insert_pr, uint16_t col_offset, terrier::type::TypeId type,
                     csv::CSVField *field);

 private:
  // Postgres NULL string
  static constexpr const char *null_string = "\\N";
  exec::ExecutionContext *exec_ctx_;
  terrier::storage::BlockStore *store_;
  terrier::catalog::namespace_oid_t ns_oid_;
};
}  // namespace terrier::sql
