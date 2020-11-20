#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/table_generator/table_reader.h"
#include "parser/expression/constant_value_expression.h"
#include "runner/mini_runners_data_config.h"
#include "runner/mini_runners_settings.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::sql {

// Keep small so that nested loop join won't take too long.
/**
 * Size of the first table
 */
constexpr uint32_t TEST1_SIZE = 10000;
/**
 * Size of the second table
 */
constexpr uint32_t TEST2_SIZE = 1000;

/**
 * Size of the alltypes table
 */
constexpr uint32_t TABLE_ALLTYPES_SIZE = 1000;

/**
 * Size of the index test table
 */
constexpr uint32_t INDEX_TEST_SIZE = 400000;

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
  explicit TableGenerator(exec::ExecutionContext *exec_ctx, common::ManagedPointer<storage::BlockStore> store,
                          catalog::namespace_oid_t ns_oid)
      : exec_ctx_{exec_ctx}, store_{store}, ns_oid_{ns_oid} {}

  /**
   * Generate table name
   * @param type Type
   * @param col Number of columns
   * @param row Number of rows
   * @param car Cardinality
   * @return table name
   */
  static std::string GenerateTableName(type::TypeId type, size_t col, size_t row, size_t car) {
    std::stringstream table_name;
    auto type_name = type::TypeUtil::TypeIdToString(type);
    table_name << type_name << "Col" << col << "Row" << row << "Car" << car;
    return table_name.str();
  }

  /**
   * Generate Mixed Table Name
   * @param types Number of types
   * @param cols Number of columns per type
   * @param row Number of rows
   * @param car Cardinality
   * @return table name
   */
  static std::string GenerateMixedTableName(std::vector<type::TypeId> types, std::vector<uint32_t> cols, size_t row,
                                            size_t car) {
    std::stringstream table_name;
    for (size_t idx = 0; idx < cols.size(); idx++) {
      table_name << type::TypeUtil::TypeIdToString(types[idx]);
      table_name << "Col" << cols[idx];
    }
    table_name << "Row" << row << "Car" << car;
    return table_name.str();
  }

  /**
   * Generate table name that contains an index
   * @param type Type
   * @param row Number of rows
   * @return table name
   */
  static std::string GenerateTableIndexName(type::TypeId type, size_t row) {
    std::stringstream table_name;
    auto type_name = type::TypeUtil::TypeIdToString(type);
    table_name << type_name << "IndexRow" << row;
    return table_name.str();
  }

  /**
   * Generate test tables.
   */
  void GenerateTestTables();

  /**
   * Generate the tables for the mini runner
   * @param settings Mini-runners settings
   * @param config Data Configuration for mini-runners
   */
  void GenerateMiniRunnersData(const runner::MiniRunnersSettings &settings,
                               const runner::MiniRunnersDataConfig &config);

  /**
   * Generate mini runners indexes
   * @param settings Mini-runners settings
   * @param config Data Configuration for mini-runners
   */
  void GenerateMiniRunnerIndexTables(const runner::MiniRunnersSettings &settings,
                                     const runner::MiniRunnersDataConfig &config);

  /**
   * Adds a mini-runner index
   * Function does not check whether an index of the same key_num
   * already exists on the table GenerateTableIndexName(type, row_num)
   *
   * @param type Datatype of the underlying table
   * @param row_num # of rows in the underlying table
   * @param key_num Number of keys comprising the index
   */
  void BuildMiniRunnerIndex(type::TypeId type, int64_t row_num, int64_t key_num);

  /**
   * Drops a unique mini-runner index
   *
   * @param type Datatype of the underlying table
   * @param row_num # of rows in the underlying table
   * @param key_num Number of keys comprising the index
   * @returns bool indicating whether successful
   */
  bool DropMiniRunnerIndex(type::TypeId type, int64_t row_num, int64_t key_num);

 private:
  exec::ExecutionContext *exec_ctx_;
  const common::ManagedPointer<storage::BlockStore> store_;
  catalog::namespace_oid_t ns_oid_;

  /**
   * Enumeration to characterize the distribution of values in a given column
   */
  enum class Dist : uint8_t { Uniform, Serial, Rotate };

  /**
   * Metadata about the data for a given column. Specifically, the type of the
   * column, the distribution of values, a min and max if appropriate.
   */
  struct ColumnInsertMeta {
    /**
     * Name of the column
     */
    std::string name_;
    /**
     * Type of the column
     */
    type::TypeId type_;
    /**
     * Whether the column is nullable
     */
    bool nullable_;
    /**
     * Distribution of values
     */
    Dist dist_;
    /**
     * Serial Counter
     */
    uint64_t counter_{0};
    /**
     * Min value of the column
     */
    uint64_t min_;
    /**
     * Max value of the column
     */
    uint64_t max_;
    /**
     * Counter to generate serial data
     */
    uint64_t serial_counter_{0};
    /**
     * Whether is copy
     */
    bool is_clone_ = false;
    /**
     * Clone idx
     */
    size_t clone_idx_ = 0;

    /**
     * Constructor
     */
    ColumnInsertMeta(std::string name, const type::TypeId type, bool nullable, Dist dist, uint64_t min, uint64_t max)
        : name_(std::move(name)), type_(type), nullable_(nullable), dist_(dist), min_(min), max_(max) {}

    /**
     * Clone Constructor
     */
    ColumnInsertMeta(std::string name, const type::TypeId type, bool nullable, size_t clone_idx)
        : name_(std::move(name)), type_(type), nullable_(nullable), is_clone_(true), clone_idx_(clone_idx) {}
  };

  /**
   * Metadata about a table. Specifically, the schema and number of
   * rows in the table.
   */
  struct TableInsertMeta {
    /**
     * Name of the table
     */
    std::string name_;
    /**
     * Number of rows
     */
    uint32_t num_rows_;
    /**
     * Columns
     */
    std::vector<ColumnInsertMeta> col_meta_;

    /**
     * Constructor
     */
    TableInsertMeta(std::string name, uint32_t num_rows, std::vector<ColumnInsertMeta> col_meta)
        : name_(std::move(name)), num_rows_(num_rows), col_meta_(std::move(col_meta)) {}
  };

  /**
   * Metadata about an index column
   */
  struct IndexColumn {
    /**
     * Name of the column
     */
    const char *name_;
    /**
     * Type of the column
     */
    const type::TypeId type_;
    /**
     * Whether the columns is nullable
     */
    bool nullable_;
    /**
     * Column name in the original table
     */
    const char *table_col_name_;

    /**
     * Constructor
     */
    IndexColumn(const char *name, const type::TypeId type, bool nullable, const char *table_col_name)
        : name_(name), type_(type), nullable_(nullable), table_col_name_(table_col_name) {}
  };

  /**
   * Metadata about an index.
   */
  struct IndexInsertMeta {
    /**
     * Name of the index
     */
    const char *index_name_;
    /**
     * Name of the corresponding table
     */
    const char *table_name_;
    /**
     * Columns
     */
    std::vector<IndexColumn> cols_;

    /**
     * Constructors
     */
    IndexInsertMeta(const char *index_name, const char *table_name, std::vector<IndexColumn> cols)
        : index_name_(index_name), table_name_(table_name), cols_(std::move(cols)) {}
  };

  void InitTestIndexes();

  /**
   * Create integer data with the given distribution
   * @tparam T
   * @param col_meta
   * @param num_vals
   * @return
   */
  template <typename T>
  T *CreateNumberColumnData(ColumnInsertMeta *col_meta, uint32_t num_vals);

  /**
   * Create an array of boolean data with the given distribution
   * @param col_meta
   * @param num_vals
   * @return
   */
  bool *CreateBooleanColumnData(ColumnInsertMeta *col_meta, uint32_t num_vals);

  /**
   * Generate column data
   * @param col_meta
   * @param num_rows
   * @return
   */
  std::pair<byte *, uint32_t *> GenerateColumnData(ColumnInsertMeta *col_meta, uint32_t num_rows);

  /**
   * Generate column data for varchar
   * @param col_meta
   * @param num_vals
   * @return
   */
  storage::VarlenEntry *CreateVarcharColumnData(ColumnInsertMeta *col_meta, uint32_t num_vals);

  /**
   * Clone Column Data
   * T - underlying type of original
   * S - underlying type of copied data
   * @param orig original
   * @param num_rows number of rows
   * @returns cloned column data
   */
  template <typename T, typename S>
  std::pair<byte *, uint32_t *> CloneColumnData(std::pair<byte *, uint32_t *> orig, uint32_t num_rows);

  /**
   * Create table
   * @param metadata TableInsertMeta
   */
  void CreateTable(TableInsertMeta *metadata);

  /**
   * Create Index
   * @param index_meta Index Metadata
   */
  void CreateIndex(IndexInsertMeta *index_meta);

  /**
   * Fill a given table according to its metadata
   * @param table_oid
   * @param table
   * @param schema
   * @param table_meta
   */
  void FillTable(catalog::table_oid_t table_oid, common::ManagedPointer<storage::SqlTable> table,
                 const catalog::Schema &schema, TableInsertMeta *table_meta);

  void FillIndex(common::ManagedPointer<storage::index::Index> index, const catalog::IndexSchema &index_schema,
                 const IndexInsertMeta &index_meta, common::ManagedPointer<storage::SqlTable> table,
                 const catalog::Schema &table_schema);

  noisepage::parser::ConstantValueExpression DummyCVE() {
    return noisepage::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
  }
};

}  // namespace noisepage::execution::sql
