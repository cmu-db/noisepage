#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "transaction/transaction_context.h"
#include "type/transient_value_factory.h"
#include "parser/expression/constant_value_expression.h"
#include "execution/table_generator/table_reader.h"

namespace terrier::execution::sql {

// Keep small so that nested loop join won't take too long.
/**
 * Size of the first table
 */
constexpr uint32_t test1_size = 10000;
/**
 * Size of the second table
 */
constexpr uint32_t test2_size = 1000;

/**
 * Helper class to generate test tables and their indexes.
 */
class TableGenerator {
 public:
  /**
   * Constructor
   * @param exec_ctx execution context of the test
   * @param store block store to use when creating tables
   * @param ns_oid oid of the namespace
   */
  explicit TableGenerator(exec::ExecutionContext *exec_ctx, storage::BlockStore *store, catalog::namespace_oid_t ns_oid)
      : exec_ctx_{exec_ctx}, store_{store}, ns_oid_{ns_oid}, table_reader_{exec_ctx, store, ns_oid} {}

  /**
   * Generate test tables.
   */
  void GenerateTestTables();

  /**
* Generate a table given its schema and data
* @param schema_file schema file name
* @param data_file data file name
*/
  void GenerateTableFromFile(const std::string &schema_file, const std::string &data_file);

  /**
   * Generate tpch tables.
   */
  void GenerateTPCHTables(const std::string &dir_name);

 private:
  exec::ExecutionContext *exec_ctx_;
  storage::BlockStore *store_;
  catalog::namespace_oid_t ns_oid_;
  TableReader table_reader_;

  /**
   * Enumeration to characterize the distribution of values in a given column
   */
  enum class Dist : uint8_t { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial };

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
    const type::TypeId type_;
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
    uint64_t min;
    /**
     * Max value of the column
     */
    uint64_t max;
    /**
     * Counter to generate serial data
     */
    uint64_t serial_counter{0};

    /**
     * Constructor
     */
    ColumnInsertMeta(const char *name, const type::TypeId type, bool nullable, Dist dist, uint64_t min, uint64_t max)
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
    uint32_t num_rows;
    /**
     * Columns
     */
    std::vector<ColumnInsertMeta> col_meta;

    /**
     * Constructor
     */
    TableInsertMeta(const char *name, uint32_t num_rows, std::vector<ColumnInsertMeta> col_meta)
        : name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
  };

  /**
   * Metadata about an index column
   */
  struct IndexColumn {
    /**
     * Name of the column
     */
    const char *name;
    /**
     * Type of the column
     */
    const type::TypeId type;
    /**
     * Whether the columns is nullable
     */
    bool nullable;
    /**
     * Column name in the original table
     */
    const char *table_col_name;

    /**
     * Constructor
     */
    IndexColumn(const char *name, const type::TypeId type, bool nullable, const char *table_col_name)
        : name(name), type(type), nullable(nullable), table_col_name(table_col_name) {}
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
  T *CreateNumberColumnData(Dist dist, uint32_t num_vals, uint64_t serial_counter, uint64_t min, uint64_t max);

  // Generate column data
  std::pair<byte *, uint32_t *> GenerateColumnData(const ColumnInsertMeta &col_meta, uint32_t num_rows);

  // Fill a given table according to its metadata
  void FillTable(catalog::table_oid_t table_oid, common::ManagedPointer<storage::SqlTable> table,
                 const catalog::Schema &schema, const TableInsertMeta &table_meta);

  void FillIndex(common::ManagedPointer<storage::index::Index> index, const catalog::IndexSchema &index_schema,
                 const IndexInsertMeta &index_meta, common::ManagedPointer<storage::SqlTable> table,
                 const catalog::Schema &table_schema);

  terrier::parser::ConstantValueExpression DummyCVE() {
    return terrier::parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }
};

}  // namespace terrier::execution::sql
