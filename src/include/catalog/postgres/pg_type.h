#pragma once

#include <array>

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
   * Column names of the form "TYP[name]_COL_OID" are present in the PostgreSQL
   * catalog specification and columns of the form "TYP_[name]_COL_OID" are
   * noisepage-specific addtions (generally pointers to internal objects).
   */
  static constexpr col_oid_t TYPOID_COL_OID = col_oid_t(1);        // INTEGER (pkey)
  static constexpr col_oid_t TYPNAME_COL_OID = col_oid_t(2);       // VARCHAR
  static constexpr col_oid_t TYPNAMESPACE_COL_OID = col_oid_t(3);  // INTEGER (fkey: pg_namespace)
  static constexpr col_oid_t TYPLEN_COL_OID = col_oid_t(4);        // SMALLINT
  static constexpr col_oid_t TYPBYVAL_COL_OID = col_oid_t(5);      // BOOLEAN
  static constexpr col_oid_t TYPTYPE_COL_OID = col_oid_t(6);       // CHAR

  static constexpr uint8_t NUM_PG_TYPE_COLS = 6;

  static constexpr std::array<col_oid_t, NUM_PG_TYPE_COLS> PG_TYPE_ALL_COL_OIDS = {
      TYPOID_COL_OID, TYPNAME_COL_OID, TYPNAMESPACE_COL_OID, TYPLEN_COL_OID, TYPBYVAL_COL_OID, TYPTYPE_COL_OID};
};

}  // namespace noisepage::catalog::postgres
