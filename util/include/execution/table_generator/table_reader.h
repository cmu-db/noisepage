#pragma once
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/table_generator/schema_reader.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

// Forward declaration
namespace csv {
class CSVField;
}

namespace noisepage::execution::sql {
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
  explicit TableReader(exec::ExecutionContext *exec_ctx, storage::BlockStore *store, catalog::namespace_oid_t ns_oid)
      : exec_ctx_{exec_ctx}, store_{store}, ns_oid_{ns_oid} {}

  /**
   * Read a table given a schema file and a data file
   * @param schema_file file containing the schema
   * @param data_file csv file containing the data
   * @return
   */
  uint32_t ReadTable(const std::string &schema_file, const std::string &data_file);

 private:
  // Create table
  catalog::table_oid_t CreateTable(TableInfo *info);

  // Create indexes
  void CreateIndexes(TableInfo *info, catalog::table_oid_t table_oid);

  // Writes a column according to its type.
  void WriteTableCol(storage::ProjectedRow *insert_pr, uint16_t col_offset, type::TypeId type, csv::CSVField *field);

  // Write an index entry
  void WriteIndexEntry(IndexInfo *index_info, storage::ProjectedRow *table_pr,
                       const std::vector<uint16_t> &table_offsets, const storage::TupleSlot &slot);

 private:
  // Postgres NULL string
  static constexpr const char *NULL_STRING = "\\N";
  exec::ExecutionContext *exec_ctx_;
  storage::BlockStore *store_;
  catalog::namespace_oid_t ns_oid_;
};
}  // namespace noisepage::execution::sql
