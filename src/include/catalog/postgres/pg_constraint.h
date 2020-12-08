#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgConstraintImpl;

/** The OIDs used by the NoisePage version of pg_constraint. */
class PgConstraint {
 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgConstraintImpl;

  static constexpr table_oid_t CONSTRAINT_TABLE_OID = table_oid_t(61);
  static constexpr index_oid_t CONSTRAINT_OID_INDEX_OID = index_oid_t(62);
  static constexpr index_oid_t CONSTRAINT_NAME_INDEX_OID = index_oid_t(63);
  static constexpr index_oid_t CONSTRAINT_NAMESPACE_INDEX_OID = index_oid_t(64);
  static constexpr index_oid_t CONSTRAINT_TABLE_INDEX_OID = index_oid_t(65);
  static constexpr index_oid_t CONSTRAINT_INDEX_INDEX_OID = index_oid_t(66);
  static constexpr index_oid_t CONSTRAINT_FOREIGNTABLE_INDEX_OID = index_oid_t(67);

  /*
   * Column names of the form "CON[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "CON_[name]" are
   * noisepage-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<constraint_oid_t, uint32_t> CONOID{col_oid_t{1}};  // INTEGER (pkey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> CONNAME{col_oid_t{2}};       // VARCHAR
  static constexpr CatalogColumnDef<namespace_oid_t, uint32_t> CONNAMESPACE{
      col_oid_t{3}};                                                                  // INTEGER (fkey: pg_namespace)
  static constexpr CatalogColumnDef<char, uint8_t> CONTYPE{col_oid_t{4}};             // CHAR
  static constexpr CatalogColumnDef<bool> CONDEFERRABLE{col_oid_t{5}};                // BOOLEAN
  static constexpr CatalogColumnDef<bool> CONDEFERRED{col_oid_t{6}};                  // BOOLEAN
  static constexpr CatalogColumnDef<bool> CONVALIDATED{col_oid_t{7}};                 // BOOLEAN
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> CONRELID{col_oid_t{8}};    // INTEGER (fkey: pg_class)
  static constexpr CatalogColumnDef<index_oid_t, uint32_t> CONINDID{col_oid_t{9}};    // INTEGER (fkey: pg_class)
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> CONFRELID{col_oid_t{10}};  // INTEGER (fkey: pg_class)
  static constexpr CatalogColumnDef<void *, uint64_t> CONBIN{col_oid_t{11}};      // BIGINT (assumes 64-bit pointers)
  static constexpr CatalogColumnDef<storage::VarlenEntry> CONSRC{col_oid_t{12}};  // VARCHAR

  static constexpr uint8_t NUM_PG_CONSTRAINT_COLS = 12;

  static constexpr std::array<col_oid_t, NUM_PG_CONSTRAINT_COLS> PG_CONSTRAINT_ALL_COL_OIDS = {
      CONOID.oid_,       CONNAME.oid_,  CONNAMESPACE.oid_, CONTYPE.oid_,   CONDEFERRABLE.oid_, CONDEFERRED.oid_,
      CONVALIDATED.oid_, CONRELID.oid_, CONINDID.oid_,     CONFRELID.oid_, CONBIN.oid_,        CONSRC.oid_};

  enum class ConstraintType : char {
    CHECK = 'c',
    FOREIGN_KEY = 'f',
    PRIMARY_KEY = 'p',
    UNIQUE = 'u',
    TRIGGER = 't',
    EXCLUSION = 'x',
  };
};

}  // namespace noisepage::catalog::postgres
