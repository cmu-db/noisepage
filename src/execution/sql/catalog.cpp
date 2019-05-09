#include "execution/sql/catalog.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/sql/data_types.h"
#include "execution/sql/schema.h"
#include "execution/sql/table.h"

#include "loggers/execution_logger.h"

namespace tpl::sql {

namespace {

/**
 * Enumeration to characterize the distribution of values in a given column
 */
enum class Dist : u8 { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial };

/**
 * Metadata about the data for a given column. Specifically, the type of the
 * column, the distribution of values, a min and max if appropriate.
 */
struct ColumnInsertMeta {
  const char *name;
  const Type &type;
  Dist dist;
  u64 min;
  u64 max;

  ColumnInsertMeta(const char *name, const Type &type, Dist dist, u64 min,
                   u64 max)
      : name(name), type(type), dist(dist), min(min), max(max) {}
};

/**
 * Metadata about data within a table. Specifically, the schema and number of
 * rows in the table.
 */
struct TableInsertMeta {
  TableId id;
  const char *name;
  u32 num_rows;
  std::vector<ColumnInsertMeta> col_meta;

  TableInsertMeta(TableId id, const char *name, u32 num_rows,
                  std::vector<ColumnInsertMeta> col_meta)
      : id(id), name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
};

/**
 * This array configures each of the test tables. When the catalog is created,
 * it bootstraps itself with the tables in this array. Each able is configured
 * with a name, size, and schema. We also configure the columns of the table. If
 * you add a new table, set it up here.
 */
// clang-format off
TableInsertMeta insert_meta[] = {
    // The empty table
    {TableId::EmptyTable, "empty_table", 0,
     {{"colA", sql::IntegerType::Instance(false), Dist::Serial, 0, 0}}},

    // Table 1
    {TableId::Test1, "test_1", 2000000,
     {{"colA", sql::IntegerType::Instance(false), Dist::Serial, 0, 0},
      {"colB", sql::IntegerType::Instance(false), Dist::Uniform, 0, 9},
      {"colC", sql::IntegerType::Instance(false), Dist::Uniform, 0, 9999},
      {"colD", sql::IntegerType::Instance(false), Dist::Uniform, 0, 99999}}},
};
// clang-format on

template <typename T>
T *CreateNumberColumnData(Dist dist, u32 num_vals, u64 min, u64 max) {
  static u64 serial_counter = 0;
  auto *val = static_cast<T *>(malloc(sizeof(T) * num_vals));

  switch (dist) {
    case Dist::Uniform: {
      std::mt19937 generator;
      std::uniform_int_distribution<T> distribution(min, max);

      for (u32 i = 0; i < num_vals; i++) {
        val[i] = distribution(generator);
      }

      break;
    }
    case Dist::Serial: {
      for (u32 i = 0; i < num_vals; i++) {
        val[i] = serial_counter++;
      }
      break;
    }
    default:
      throw std::runtime_error("Implement me!");
  }

  return val;
}

std::pair<byte *, u32 *> GenerateColumnData(const ColumnInsertMeta &col_meta,
                                            u32 num_rows) {
  // Create data
  byte *col_data = nullptr;
  switch (col_meta.type.type_id()) {
    case TypeId::Boolean: {
      throw std::runtime_error("Implement me!");
    }
    case TypeId::SmallInt: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i16>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::Integer: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i32>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::BigInt:
    case TypeId::Decimal: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i64>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::Date:
    case TypeId::Char:
    case TypeId::Varchar: {
      throw std::runtime_error("Implement me!");
    }
  }

  // Create bitmap
  u32 *null_bitmap = nullptr;
  if (col_meta.type.nullable()) {
    TPL_ASSERT(num_rows != 0, "Cannot have 0 rows.");
    u64 num_words = util::BitUtil::Num32BitWordsFor(num_rows);
    null_bitmap = static_cast<u32 *>(malloc(num_words * sizeof(u32)));
  }

  return {col_data, null_bitmap};
}

void InitTable(const TableInsertMeta &table_meta, Table *table) {
  EXECUTION_LOG_INFO("Populating table instance '{}' with {} rows", table_meta.name,
           table_meta.num_rows);

  u32 batch_size = 10000;
  u32 num_batches = table_meta.num_rows / batch_size +
                    static_cast<u32>(table_meta.num_rows % batch_size != 0);

  for (u32 i = 0; i < num_batches; i++) {
    std::vector<ColumnSegment> columns;

    // Generate column data for all columns
    u32 num_vals = std::min(batch_size, table_meta.num_rows - (i * batch_size));
    TPL_ASSERT(num_vals != 0, "Can't have empty columns.");
    for (const auto &col_meta : table_meta.col_meta) {
      auto [data, null_bitmap] = GenerateColumnData(col_meta, num_vals);
      // NOLINTNEXTLINE(clang-analyzer-unix.Malloc)
      columns.emplace_back(col_meta.type, data, null_bitmap, num_vals);
    }

    // Insert into table
    table->Insert(Table::Block(std::move(columns), num_vals));
  }
}

}  // namespace

/*
 * Create a catalog, setting up all tables.
 */
Catalog::Catalog() {
  EXECUTION_LOG_INFO("Initializing catalog");

  // Insert tables into catalog
  for (const auto &meta : insert_meta) {
    EXECUTION_LOG_INFO("Creating table instance '{}' in catalog", meta.name);

    std::vector<Schema::ColumnInfo> cols;
    for (const auto &col_meta : meta.col_meta) {
      cols.emplace_back(col_meta.name, col_meta.type);
    }

    // Insert into catalog
    table_catalog_[meta.id] = std::make_unique<Table>(
        static_cast<u16>(meta.id), std::make_unique<Schema>(std::move(cols)));
  }

  // Populate all tables
  for (const auto &table_meta : insert_meta) {
    InitTable(table_meta, LookupTableById(table_meta.id));
  }

  EXECUTION_LOG_INFO("Catalog initialization complete");
}

// We need this here because a catalog has a map that stores unique pointers to
// SQL Table objects. SQL Table is forward-declared in the header file, so the
// destructor cannot be inlined in the header.
Catalog::~Catalog() = default;

Catalog *Catalog::Instance() {
  static Catalog kInstance;
  return &kInstance;
}

Table *Catalog::LookupTableByName(const std::string &name) const {
  static std::unordered_map<std::string, TableId> kTableNameMap = {
#define ENTRY(Name, Str, ...) {Str, TableId::Name},
      TABLES(ENTRY)
#undef ENTRY
  };

  auto iter = kTableNameMap.find(name);
  if (iter == kTableNameMap.end()) {
    return nullptr;
  }

  return LookupTableById(iter->second);
}

Table *Catalog::LookupTableByName(const ast::Identifier name) const {
  return LookupTableByName(name.data());
}

Table *Catalog::LookupTableById(TableId table_id) const {
  auto iter = table_catalog_.find(table_id);
  return (iter == table_catalog_.end() ? nullptr : iter->second.get());
}

}  // namespace tpl::sql
