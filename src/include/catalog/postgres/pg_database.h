#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::catalog {
class Catalog;
class DatabaseCatalog;
}  // namespace noisepage::catalog

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;

/** The OIDs used by the NoisePage version of pg_database. */
class PgDatabase {
 private:
  friend class catalog::Catalog;
  friend class storage::RecoveryManager;
  friend class Builder;

  static constexpr table_oid_t DATABASE_TABLE_OID = table_oid_t(1);
  static constexpr index_oid_t DATABASE_OID_INDEX_OID = index_oid_t(2);
  static constexpr index_oid_t DATABASE_NAME_INDEX_OID = index_oid_t(3);

  /*
   * Column names of the form "DAT[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "DAT_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<db_oid_t, uint32_t> DATOID{col_oid_t{1}};     // INTEGER (pkey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> DATNAME{col_oid_t{2}};  // VARCHAR
  static constexpr CatalogColumnDef<DatabaseCatalog *, uint64_t> DAT_CATALOG{
      col_oid_t{3}};  // BIGINT (assumes 64-bit pointers)

  static constexpr uint8_t NUM_PG_DATABASE_COLS = 3;

  static constexpr std::array<col_oid_t, NUM_PG_DATABASE_COLS> PG_DATABASE_ALL_COL_OIDS = {DATOID.oid_, DATNAME.oid_,
                                                                                           DAT_CATALOG.oid_};
};

}  // namespace noisepage::catalog::postgres
