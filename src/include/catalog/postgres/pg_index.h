#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgCoreImpl;

/** The OIDs used by the NoisePage version of pg_index. */
class PgIndex {
 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgCoreImpl;

  static constexpr table_oid_t INDEX_TABLE_OID = table_oid_t(31);
  static constexpr index_oid_t INDEX_OID_INDEX_OID = index_oid_t(32);
  static constexpr index_oid_t INDEX_TABLE_INDEX_OID = index_oid_t(33);

  /*
   * Column names of the form "IND[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "IND_[name]" are
   * noisepage-specific addtions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<index_oid_t, uint32_t> INDOID{col_oid_t{1}};    // INTEGER (pkey, fkey: pg_class)
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> INDRELID{col_oid_t{2}};  // INTEGER (fkey: pg_class)
  static constexpr CatalogColumnDef<bool> INDISUNIQUE{col_oid_t{3}};                // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDISPRIMARY{col_oid_t{4}};               // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDISEXCLUSION{col_oid_t{5}};             // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDIMMEDIATE{col_oid_t{6}};               // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDISVALID{col_oid_t{7}};                 // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDISREADY{col_oid_t{8}};                 // BOOLEAN
  static constexpr CatalogColumnDef<bool> INDISLIVE{col_oid_t{9}};                  // BOOLEAN
  static constexpr CatalogColumnDef<char, uint8_t> IND_TYPE{col_oid_t{10}};         // CHAR (see IndexSchema)

  static constexpr uint8_t NUM_PG_INDEX_COLS = 10;

  static constexpr std::array<col_oid_t, NUM_PG_INDEX_COLS> PG_INDEX_ALL_COL_OIDS = {
      INDOID.oid_,       INDRELID.oid_,   INDISUNIQUE.oid_, INDISPRIMARY.oid_, INDISEXCLUSION.oid_,
      INDIMMEDIATE.oid_, INDISVALID.oid_, INDISREADY.oid_,  INDISLIVE.oid_,    IND_TYPE.oid_};
};

}  // namespace noisepage::catalog::postgres
