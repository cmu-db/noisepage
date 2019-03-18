#pragma once
#include <storage/sql_table.h>
#include <random>
#include <vector>
#include "catalog/schema.h"
#include "common/strong_typedef.h"
#include "type/type_id.h"
#include "util/multithread_test_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"

namespace terrier {
struct CatalogTestUtil {
  // Returns a random Schema that is guaranteed to be valid for a single test instance. Multiple Schemas instantiated
  // with this function may have non-unique oids, but within a Schema, Column oids are unique.
  template <typename Random>
  static catalog::Schema RandomSchema(const uint16_t max_cols, Random *const generator) {
    TERRIER_ASSERT(max_cols > 0, "There should be at least 1 columm.");
    catalog::col_oid_t col_oid(0);
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(*generator);
    std::vector<type::TypeId> possible_attr_types{
        type::TypeId::BOOLEAN,   type::TypeId::TINYINT, type::TypeId::SMALLINT,
        type::TypeId::INTEGER,   type::TypeId::BIGINT,  type::TypeId::DECIMAL,
        type::TypeId::TIMESTAMP, type::TypeId::DATE,    type::TypeId::VARCHAR};
    std::vector<bool> possible_attr_nullable{true, false};
    std::vector<catalog::Schema::Column> columns;
    for (uint16_t i = 0; i < num_attrs; i++) {
      type::TypeId attr_type = *RandomTestUtil::UniformRandomElement(&possible_attr_types, generator);
      bool attr_nullable = *RandomTestUtil::UniformRandomElement(&possible_attr_nullable, generator);
      columns.emplace_back("col_name", attr_type, attr_nullable, col_oid++);
    }
    return catalog::Schema(columns);
  }

  // The same as RandomSchem but it won't generate Varchar Column
  template <typename Random>
  static catalog::Schema RandomSchemaNoVarchar(const uint16_t max_cols, Random *const generator) {
    TERRIER_ASSERT(max_cols > 0, "There should be at least 1 columm.");
    catalog::col_oid_t col_oid(0);
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(*generator);
    std::vector<type::TypeId> possible_attr_types{
        type::TypeId::BOOLEAN, type::TypeId::TINYINT, type::TypeId::SMALLINT,  type::TypeId::INTEGER,
        type::TypeId::BIGINT,  type::TypeId::DECIMAL, type::TypeId::TIMESTAMP, type::TypeId::DATE};
    std::vector<bool> possible_attr_nullable{true, false};
    std::vector<catalog::Schema::Column> columns;
    for (uint16_t i = 0; i < num_attrs; i++) {
      type::TypeId attr_type = *RandomTestUtil::UniformRandomElement(&possible_attr_types, generator);
      bool attr_nullable = *RandomTestUtil::UniformRandomElement(&possible_attr_nullable, generator);
      columns.emplace_back("col_name", attr_type, attr_nullable, col_oid++);
    }
    return catalog::Schema(columns);
  }

  // Return a random ProjectedRow for the given schema. It always include all columns.
  // The caller needs to free the ProjectedRow later by using
  // delete[] reinterpret_cast<byte *>(pr)
  template <typename Random>
  static storage::ProjectedRow *RandomInsertRow(storage::SqlTable *sql_table, const catalog::Schema &schema,
                                                storage::layout_version_t version, Random *const generator) {
    // insertion rows need to have all columns
    std::vector<catalog::col_oid_t> col_oids;

    for (auto &col : schema.GetColumns()) {
      col_oids.emplace_back(col.GetOid());
    }
    // populate the row with random bytes
    auto pr_pair = sql_table->InitializerForProjectedRow(col_oids, version);
    byte *buffer = common::AllocationUtil::AllocateAligned(pr_pair.first.ProjectedRowSize());
    storage::ProjectedRow *pr = pr_pair.first.InitializeRow(buffer);
    PopulateRandomRow(pr, schema, pr_pair.second, generator);

    // if a columns is nullable, flip a coin to set it to null
    std::vector<bool> include{true, false};
    for (auto &col : schema.GetColumns()) {
      if (col.GetNullable()) {
        // flip a coin
        if (*RandomTestUtil::UniformRandomElement(&include, generator)) {
          pr->SetNull(pr_pair.second.at(col.GetOid()));
        }
      }
    }
    return pr;
  }

  // It populates a projected row with random bytes. The schema must not contain VarChar
  // The projected row must contain all the columns
  template <typename Random>
  static void PopulateRandomRow(storage::ProjectedRow *row, const catalog::Schema &schema,
                                const storage::ProjectionMap &pr_map, Random *const generator) {
    for (auto &it : pr_map) {
      row->SetNotNull(it.second);
      byte *addr = row->AccessWithNullCheck(it.second);
      uint8_t size = 0;
      for (auto &col : schema.GetColumns()) {
        if (col.GetOid() == it.first) {
          size = col.GetAttrSize();
        }
      }
      StorageTestUtil::FillWithRandomBytes(size, addr, generator);
    }
  }

  // Check if two rows have the same content. It assumes they have the same ProjectionMap.
  template <class RowType1, class RowType2>
  static bool ProjectionListEqual(const catalog::Schema &schema, const RowType1 *const one, const RowType2 *const other,
                                  const storage::ProjectionMap &map) {
    if (one->NumColumns() != other->NumColumns()) return false;
    for (auto &it : map) {
      catalog::col_oid_t col_oid = it.first;
      uint8_t attr_size = 0;
      for (auto &col : schema.GetColumns()) {
        if (col.GetOid() == col_oid) {
          attr_size = col.GetAttrSize();
          break;
        }
      }

      const byte *one_content = one->AccessWithNullCheck(it.second);
      const byte *other_content = other->AccessWithNullCheck(it.second);
      // Either both are null or neither is null.
      if (one_content == nullptr || other_content == nullptr) {
        if (one_content == other_content) continue;
        return false;
      }
      // Otherwise, they should be bit-wise identical.
      if (memcmp(one_content, other_content, attr_size) != 0) return false;
    }
    return true;
  }
};
}  // namespace terrier
