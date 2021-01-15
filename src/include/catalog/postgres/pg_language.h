#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgLanguageImpl;
class PgProcImpl;

/** The OIDs used by the NoisePage version of pg_language. */
class PgLanguage {
 private:
  friend class storage::RecoveryManager;

  friend class Builder;
  friend class PgLanguageImpl;
  friend class PgProcImpl;

  static constexpr table_oid_t LANGUAGE_TABLE_OID = table_oid_t(71);
  static constexpr index_oid_t LANGUAGE_OID_INDEX_OID = index_oid_t(72);
  static constexpr index_oid_t LANGUAGE_NAME_INDEX_OID = index_oid_t(73);
  static constexpr language_oid_t INTERNAL_LANGUAGE_OID = language_oid_t(74);
  static constexpr language_oid_t PLPGSQL_LANGUAGE_OID = language_oid_t(75);

  /*
   * Column names of the form "LAN[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "LAN_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<language_oid_t, uint32_t> LANOID{col_oid_t{1}};  // INTEGER (pkey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> LANNAME{col_oid_t{2}};     // VARCHAR (skey)
  static constexpr CatalogColumnDef<bool> LANISPL{col_oid_t{3}};                     // BOOLEAN (skey)
  static constexpr CatalogColumnDef<bool> LANPLTRUSTED{col_oid_t{4}};                // BOOLEAN (skey)
  // TODO(tanujnay112): Make these foreign keys when we implement pg_proc
  static constexpr CatalogColumnDef<proc_oid_t, uint32_t> LANPLCALLFOID{
      col_oid_t{5}};                                                                   // INTEGER (skey) (fkey: pg_proc)
  static constexpr CatalogColumnDef<proc_oid_t, uint32_t> LANINLINE{col_oid_t{6}};     // INTEGER (skey) (fkey: pg_proc)
  static constexpr CatalogColumnDef<proc_oid_t, uint32_t> LANVALIDATOR{col_oid_t{7}};  // INTEGER (skey) (fkey: pg_proc)

  static constexpr uint8_t NUM_PG_LANGUAGE_COLS = 7;

  static constexpr std::array<col_oid_t, NUM_PG_LANGUAGE_COLS> PG_LANGUAGE_ALL_COL_OIDS = {
      LANOID.oid_,        LANNAME.oid_,   LANISPL.oid_,     LANPLTRUSTED.oid_,
      LANPLCALLFOID.oid_, LANINLINE.oid_, LANVALIDATOR.oid_};
};
}  // namespace noisepage::catalog::postgres
