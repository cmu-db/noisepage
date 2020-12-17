#pragma once
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "loggers/execution_logger.h"
#include "parser/expression/constant_value_expression.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

namespace noisepage::execution::sql {

// Maps from index columns to table columns.
using IndexTableMap = std::vector<uint16_t>;

/**
 * Stores info about an index
 */
struct IndexInfo {
  /**
   * Constructor
   */
  IndexInfo() = default;

  /**
   * Index Name
   */
  std::string index_name_;
  /**
   * Index Schema
   */
  std::vector<catalog::IndexSchema::Column> cols_;

  /**
   * Mapping from index column to table column
   */
  IndexTableMap index_map_;

  /**
   * Physical Index
   */
  common::ManagedPointer<storage::index::Index> index_ptr_{nullptr};

  /**
   * Precomputed offsets into the projected row
   */
  std::vector<uint16_t> offsets_{};

  /**
   * Projected row to use for inserts
   */
  storage::ProjectedRow *index_pr_;
};

/**
 * Stores table information
 */
struct TableInfo {
  /**
   * Constructor
   */
  TableInfo() = default;

  /**
   * Table Name
   */
  std::string table_name_;
  /**
   * Table Schema
   */
  std::vector<catalog::Schema::Column> cols_;

  /**
   * indexes
   */
  std::vector<std::unique_ptr<IndexInfo>> indexes_;
};

/**
 * Reads .schema file
 * File format:
 * table_name num_cols
 * col_name1(string), type1(string), nullable1(0 or 1)
 * ...
 * col_nameN(string), typeN(string), nullableN(0 or 1), varchar_size if type == varchar
 * num_indexes
 * index_name1 num_index_cols1
 * table_col_idx1 table_col_idxN
 * ...
 * ...
 * index_nameM num_index_colM
 * ...
 */
class SchemaReader {
 public:
  /**
   * Constructor
   */
  SchemaReader()
      : type_names_{{"tinyint", type::TypeId::TINYINT}, {"smallint", type::TypeId::SMALLINT},
                    {"int", type::TypeId::INTEGER},     {"bigint", type::TypeId::BIGINT},
                    {"bool", type::TypeId::BOOLEAN},    {"real", type::TypeId::REAL},
                    {"decimal", type::TypeId::REAL},    {"varchar", type::TypeId::VARCHAR},
                    {"varlen", type::TypeId::VARCHAR},  {"date", type::TypeId::DATE}} {}

  /**
   * Reads table metadata
   * @param filename name of the file containing the metadate
   * @return the struct containing information about the table
   */
  std::unique_ptr<TableInfo> ReadTableInfo(const std::string &filename) {
    // Allocate table information
    auto table_info = std::make_unique<TableInfo>();
    // Open file to read
    std::ifstream schema_file;
    schema_file.open(filename);
    // Read Table name and num_cols
    uint32_t num_cols;
    schema_file >> table_info->table_name_ >> num_cols;
    EXECUTION_LOG_TRACE("Reading table {} with {} columns", table_info->table_name_, num_cols);
    // Read columns & create table schema
    table_info->cols_ = ReadColumns(&schema_file, num_cols);
    // Read num_indexes & create index information
    uint32_t num_indexes;
    schema_file >> num_indexes;
    ReadIndexSchemas(&schema_file, table_info.get(), num_indexes);
    return table_info;
  }

 private:
  // Read index schemas
  void ReadIndexSchemas(std::ifstream *in, TableInfo *table_info, uint32_t num_indexes) {
    uint32_t num_index_cols;
    for (uint32_t i = 0; i < num_indexes; i++) {
      auto index_info = std::make_unique<IndexInfo>();
      // Read index name and num_index_cols
      *in >> index_info->index_name_ >> num_index_cols;
      // Read each index column
      std::vector<catalog::IndexSchema::Column> index_cols;
      uint16_t col_idx;
      for (uint32_t j = 0; j < num_index_cols; j++) {
        *in >> col_idx;
        index_info->index_map_.emplace_back(col_idx);
        const auto &table_column = table_info->cols_[col_idx];
        index_info->cols_.emplace_back("index_col" + std::to_string(col_idx), table_column.Type(),
                                       table_column.Nullable(), parser::ConstantValueExpression(table_column.Type()));
      }
      // Update list of indexes
      table_info->indexes_.emplace_back(std::move(index_info));
    }
  }

  // Read columns
  std::vector<catalog::Schema::Column> ReadColumns(std::ifstream *in, uint32_t num_cols) {
    std::vector<catalog::Schema::Column> cols;
    // Read each column
    std::string col_name;
    std::string col_type_str;
    type::TypeId col_type;
    uint32_t varchar_size{0};
    bool nullable;
    for (uint32_t i = 0; i < num_cols; i++) {
      *in >> col_name >> col_type_str >> nullable;
      col_type = type_names_.at(col_type_str);
      if (col_type == type::TypeId::VARCHAR) {
        *in >> varchar_size;
        cols.emplace_back(col_name, col_type, varchar_size, nullable, parser::ConstantValueExpression(col_type));
      } else {
        cols.emplace_back(col_name, col_type, nullable, parser::ConstantValueExpression(col_type));
      }
    }
    return cols;
  }

 private:
  // Supported types
  const std::unordered_map<std::string, type::TypeId> type_names_;
};
}  // namespace noisepage::execution::sql
