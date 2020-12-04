#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgAttributeImpl;

/** The OIDs used by the NoisePage version of pg_attribute. */
class PgAttribute {
 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgCoreImpl;

  static constexpr table_oid_t COLUMN_TABLE_OID = table_oid_t(41);
  static constexpr index_oid_t COLUMN_OID_INDEX_OID = index_oid_t(42);
  static constexpr index_oid_t COLUMN_NAME_INDEX_OID = index_oid_t(43);

  /*
   * Column names of the form "ATT[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "ATT_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<col_oid_t, uint32_t> ATTNUM{col_oid_t{1}};      // INTEGER (pkey)
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> ATTRELID{col_oid_t{2}};  // INTEGER (fkey: pg_class)
  static constexpr CatalogColumnDef<storage::VarlenEntry> ATTNAME{col_oid_t{3}};    // VARCHAR
  static constexpr CatalogColumnDef<type_oid_t, uint32_t> ATTTYPID{col_oid_t{4}};   // INTEGER (fkey: pg_type)
  static constexpr CatalogColumnDef<uint16_t> ATTLEN{col_oid_t{5}};                 // SMALLINT
  static constexpr CatalogColumnDef<int32_t> ATTTYPMOD{col_oid_t{6}};               // INTEGER
  static constexpr CatalogColumnDef<bool> ATTNOTNULL{col_oid_t{7}};                 // BOOLEAN
  // The following columns come from 'pg_attrdef' but are included here for
  // simplicity.  PostgreSQL splits out the table to allow more fine-grained
  // locking during DDL operations which is not an issue in this system
  static constexpr CatalogColumnDef<storage::VarlenEntry> ADSRC{col_oid_t{8}};  // VARCHAR

  static constexpr uint8_t NUM_PG_ATTRIBUTE_COLS = 8;

  static constexpr std::array<col_oid_t, NUM_PG_ATTRIBUTE_COLS> PG_ATTRIBUTE_ALL_COL_OIDS = {
      ATTNUM.oid_, ATTRELID.oid_,  ATTNAME.oid_,    ATTTYPID.oid_,
      ATTLEN.oid_, ATTTYPMOD.oid_, ATTNOTNULL.oid_, ADSRC.oid_};
};

}  // namespace noisepage::catalog::postgres
