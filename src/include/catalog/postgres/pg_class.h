#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog {
class DatabaseCatalog;
}  // namespace noisepage::catalog

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgCoreImpl;

/** The OIDs used by the NoisePage version of pg_class. */
class PgClass {
 public:
  /** The relkind in Postgres, i.e., the kind of relation. All enum values match Postgres. */
  enum class RelKind : char {
    REGULAR_TABLE = 'r',      ///< Ordinary table.
    INDEX = 'i',              ///< Index.
    SEQUENCE = 'S',           ///< Sequence.
    VIEW = 'v',               ///< View.
    MATERIALIZED_VIEW = 'm',  ///< Materialized view.
    COMPOSITE_TYPE = 'c',     ///< Composite type.
    TOAST_TABLE = 't',        ///< TOAST table.
    FOREIGN_TABLE = 'f',      ///< Foreign table.
  };

 private:
  friend class catalog::DatabaseCatalog;
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgCoreImpl;

  static constexpr table_oid_t CLASS_TABLE_OID = table_oid_t(21);
  static constexpr index_oid_t CLASS_OID_INDEX_OID = index_oid_t(22);
  static constexpr index_oid_t CLASS_NAME_INDEX_OID = index_oid_t(23);
  static constexpr index_oid_t CLASS_NAMESPACE_INDEX_OID = index_oid_t(24);

  /*
   * Column names of the form "REL[name]_COL_OID" are present in the PostgreSQL
   * catalog specification and columns of the form "REL_[name]_COL_OID" are
   * noisepage-specific addtions (generally pointers to internal objects).
   */
  static constexpr col_oid_t RELOID_COL_OID = col_oid_t(1);          // INTEGER (pkey)
  static constexpr col_oid_t RELNAME_COL_OID = col_oid_t(2);         // VARCHAR
  static constexpr col_oid_t RELNAMESPACE_COL_OID = col_oid_t(3);    // INTEGER (fkey: pg_namespace)
  static constexpr col_oid_t RELKIND_COL_OID = col_oid_t(4);         // CHAR
  static constexpr col_oid_t REL_SCHEMA_COL_OID = col_oid_t(5);      // BIGINT (assumes 64-bit pointers)
  static constexpr col_oid_t REL_PTR_COL_OID = col_oid_t(6);         // BIGINT (assumes 64-bit pointers)
  static constexpr col_oid_t REL_NEXTCOLOID_COL_OID = col_oid_t(7);  // INTEGER

  static constexpr uint8_t NUM_PG_CLASS_COLS = 7;

  static constexpr std::array<col_oid_t, NUM_PG_CLASS_COLS> PG_CLASS_ALL_COL_OIDS = {
      RELOID_COL_OID,     RELNAME_COL_OID, RELNAMESPACE_COL_OID,  RELKIND_COL_OID,
      REL_SCHEMA_COL_OID, REL_PTR_COL_OID, REL_NEXTCOLOID_COL_OID};
};

}  // namespace noisepage::catalog::postgres
