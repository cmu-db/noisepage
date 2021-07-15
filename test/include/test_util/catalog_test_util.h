#pragma once

#include <random>
#include <vector>

#include "catalog/schema.h"
#include "common/strong_typedef.h"
#include "test_util/multithread_test_util.h"
#include "test_util/random_test_util.h"
#include "test_util/storage_test_util.h"


namespace noisepage {
struct CatalogTestUtil {
  CatalogTestUtil() = delete;

  // Returns a random Schema that is guaranteed to be valid for a single test instance. Multiple Schemas instantiated
  // with this function may have non-unique oids, but within a Schema, Column oids are unique.
  template <typename Random>
  static catalog::Schema RandomSchema(const uint16_t max_cols, Random *const generator) {
    NOISEPAGE_ASSERT(max_cols > 0, "There should be at least 1 columm.");
    catalog::col_oid_t col_oid(0);
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(*generator);
    std::vector<execution::sql::SqlTypeId> possible_attr_types{
        execution::sql::SqlTypeId::Boolean,   execution::sql::SqlTypeId::TinyInt, execution::sql::SqlTypeId::SmallInt,
        execution::sql::SqlTypeId::Integer,   execution::sql::SqlTypeId::BigInt,  execution::sql::SqlTypeId::Double,
        execution::sql::SqlTypeId::Timestamp, execution::sql::SqlTypeId::Date,    execution::sql::SqlTypeId::Varchar};
    std::vector<bool> possible_attr_nullable{true, false};
    std::vector<catalog::Schema::Column> columns;
    for (uint16_t i = 0; i < num_attrs; i++) {
      execution::sql::SqlTypeId attr_type = *RandomTestUtil::UniformRandomElement(&possible_attr_types, generator);
      bool attr_nullable = *RandomTestUtil::UniformRandomElement(&possible_attr_nullable, generator);
      if (attr_type != execution::sql::SqlTypeId::Varchar && attr_type != execution::sql::SqlTypeId::Varbinary)
        columns.emplace_back("col_name", attr_type, attr_nullable, col_oid++);
      else
        columns.emplace_back("col_name", attr_type, 255, attr_nullable, col_oid++);
    }
    return catalog::Schema(columns);
  }

  // Define contants here for when you need to fake having a catalog in tests. These values may change as the catalog
  // comes in, and tests should be modified accordingly.
  static constexpr catalog::db_oid_t TEST_DB_OID = catalog::db_oid_t(101);
  static constexpr catalog::table_oid_t TEST_TABLE_OID = catalog::table_oid_t(102);
  static constexpr catalog::namespace_oid_t TEST_NAMESPACE_OID = catalog::namespace_oid_t(103);
  static constexpr catalog::index_oid_t TEST_INDEX_OID = catalog::index_oid_t(104);
};
}  // namespace noisepage
