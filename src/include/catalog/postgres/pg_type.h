#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgTypeImpl;

/** The OIDs used by the NoisePage version of pg_type. */
class PgType {
 public:
  /** The category of the type. */
  enum class Type : char {
    BASE = 'b',       ///< Base type.
    COMPOSITE = 'c',  ///< Composite type.
    PG_DOMAIN = 'd',  ///< Domain type.
    ENUM = 'e',       ///< Enumeration type.
    PSEUDO = 'p',     ///< Pseudo type.
    RANGE = 'r',      ///< Range type.
  };

 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgTypeImpl;

  static constexpr table_oid_t TYPE_TABLE_OID = table_oid_t(51);
  static constexpr index_oid_t TYPE_OID_INDEX_OID = index_oid_t(52);
  static constexpr index_oid_t TYPE_NAME_INDEX_OID = index_oid_t(53);
  static constexpr index_oid_t TYPE_NAMESPACE_INDEX_OID = index_oid_t(54);

  /*
   * Column names of the form "TYP[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "TYP_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<type_oid_t> TYPOID{col_oid_t{1}};             // INTEGER (pkey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> TYPNAME{col_oid_t{2}};  // VARCHAR
  static constexpr CatalogColumnDef<namespace_oid_t> TYPNAMESPACE{col_oid_t{3}};  // INTEGER (fkey: pg_namespace)
  static constexpr CatalogColumnDef<int16_t> TYPLEN{col_oid_t{4}};                // SMALLINT
  static constexpr CatalogColumnDef<bool> TYPBYVAL{col_oid_t{5}};                 // BOOLEAN
  static constexpr CatalogColumnDef<uint8_t> TYPTYPE{col_oid_t{6}};               // CHAR

  static constexpr uint8_t NUM_PG_TYPE_COLS = 6;

  static constexpr std::array<col_oid_t, NUM_PG_TYPE_COLS> PG_TYPE_ALL_COL_OIDS = {
      TYPOID.oid_, TYPNAME.oid_, TYPNAMESPACE.oid_, TYPLEN.oid_, TYPBYVAL.oid_, TYPTYPE.oid_};
};

}  // namespace noisepage::catalog::postgres
