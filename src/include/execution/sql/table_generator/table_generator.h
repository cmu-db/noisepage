#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/table_generator/table_reader.h"
#include "transaction/transaction_context.h"

namespace tpl::sql {

// Keep small so that nested loop join won't take too long.
/**
 * Size of the first table
 */
constexpr u32 test1_size = 10000;
/**
 * Size of the second table
 */
constexpr u32 test2_size = 1000;

/**
 * Helper class to generate test tables and their indexes.
 * There are three ways to generate tables:
 * 1. Call GenerateTableFromFile with a .schema file and a .data (csv) files.
 * 2. Call GenerateTestTables. To create more tables with this method, modify the insert_meta variable (to create more
 * tables) or the index_metas variables (to create more indexes).
 * 3. Call GenerateTpchTables to generate tpch tables.
 * TODO(Amadou): Add GenerateTablesFromDir. This will read all .schema files and .data (csv) files in a directory.
 * This requires std::filesystem or boost/filesystem though.
 */
class TableGenerator {
 public:
  /**
   * Constructor
   * @param exec_ctx execution context of the test
   */
  explicit TableGenerator(exec::ExecutionContext *exec_ctx, terrier::storage::BlockStore * store, terrier::catalog::namespace_oid_t ns_oid) : exec_ctx_{exec_ctx}, store_{store}, ns_oid_{ns_oid}, table_reader{exec_ctx, store, ns_oid} {}

  /**
   * Generate the tables withing a directory
   * @param dir_name directory name
   */
  void GenerateTablesFromDir(const std::string &dir_name);

  /**
   * Generate a table given its schema and data
   * @param schema_file schema file name
   * @param data_file data file name
   */
  void GenerateTableFromFile(const std::string &schema_file, const std::string &data_file);

  /**
   * Generate static test tables below.
   */
  void GenerateTestTables();

  /**
   * Generate tpch tables.
   */
  void GenerateTPCHTables(const std::string &dir_name);

 private:
  exec::ExecutionContext *exec_ctx_;
  terrier::storage::BlockStore * store_;
  terrier::catalog::namespace_oid_t ns_oid_;
  TableReader table_reader;

  /**
   * Enumeration to characterize the distribution of values in a given column
   */
  enum class Dist : u8 { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial };

  /**
   * Metadata about the data for a given column. Specifically, the type of the
   * column, the distribution of values, a min and max if appropriate.
   */
  struct ColumnInsertMeta {
    /**
     * Name of the column
     */
    const char *name;
    /**
     * Type of the column
     */
    const terrier::type::TypeId type_;
    /**
     * Whether the column is nullable
     */
    bool nullable;
    /**
     * Distribution of values
     */
    Dist dist;
    /**
     * Min value of the column
     */
    u64 min;
    /**
     * Max value of the column
     */
    u64 max;

    /**
     * Constructor
     */
    ColumnInsertMeta(const char *name, const terrier::type::TypeId type, bool nullable, Dist dist, u64 min, u64 max)
        : name(name), type_(type), nullable(nullable), dist(dist), min(min), max(max) {}
  };

  /**
   * Metadata about a table. Specifically, the schema and number of
   * rows in the table.
   */
  struct TableInsertMeta {
    /**
     * Name of the table
     */
    const char *name;
    /**
     * Number of rows
     */
    u32 num_rows;
    /**
     * Columns
     */
    std::vector<ColumnInsertMeta> col_meta;

    /**
     * Constructor
     */
    TableInsertMeta(const char *name, u32 num_rows, std::vector<ColumnInsertMeta> col_meta)
        : name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
  };

  /**
   * Metadata about an index column
   */
  struct IndexColumn {
    /**
     * Name of the column
     */
    const char * name_;

    /**
     * Type of the column
     */
    const terrier::type::TypeId type_;
    /**
     * Whether the columns is nullable
     */
    bool nullable_;
    /**
     * Index in the original table
     */
    uint32_t table_col_idx_;

    /**
     * Constructor
     */
    IndexColumn(const char * name, const terrier::type::TypeId type, bool nullable, uint32_t table_col_idx)
        : name_(name), type_(type), nullable_(nullable), table_col_idx_(table_col_idx) {}
  };

  /**
   * Metadata about an index.
   */
  struct IndexInsertMeta {
    /**
     * Name of the index
     */
    const char *index_name;
    /**
     * Name of the corresponding table
     */
    const char *table_name;
    /**
     * Columns
     */
    std::vector<IndexColumn> cols;

    /**
     * Constructors
     */
    IndexInsertMeta(const char *index_name, const char *table_name, std::vector<IndexColumn> cols)
        : index_name(index_name), table_name(table_name), cols(std::move(cols)) {}
  };

  void InitTestIndexes();

  // Create integer data with the given distribution
  template <typename T>
  T *CreateNumberColumnData(Dist dist, u32 num_vals, u64 min, u64 max);

  // Generate column data
  std::pair<byte *, u32 *> GenerateColumnData(const ColumnInsertMeta &col_meta, u32 num_rows);

  // Fill a given table according to its metadata
  void FillTable(terrier::catalog::table_oid_t table_oid, terrier::common::ManagedPointer<terrier::storage::SqlTable> table, const terrier::catalog::Schema & schema, const TableInsertMeta &table_meta);

  void FillIndex(terrier::common::ManagedPointer<terrier::storage::index::Index> index, const terrier::catalog::IndexSchema & index_schema, const IndexInsertMeta & index_meta, terrier::common::ManagedPointer<terrier::storage::SqlTable> table, const terrier::catalog::Schema & table_schema);

  terrier::parser::ConstantValueExpression DummyCVE() {
    return terrier::parser::ConstantValueExpression(terrier::type::TransientValueFactory::GetInteger(0));
  }
};

}  // namespace tpl::sql
