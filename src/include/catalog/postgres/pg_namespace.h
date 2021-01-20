#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage {
class CatalogTests_CatalogSearchPathTest_Test;
}  // namespace noisepage

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgConstraintImpl;
class PgCoreImpl;
class PgLanguageImpl;
class PgProcImpl;
class PgTypeImpl;

/** The OIDs used by the NoisePage version of pg_namespace. */
class PgNamespace {
 public:
  static constexpr namespace_oid_t NAMESPACE_CATALOG_NAMESPACE_OID = namespace_oid_t(14);  ///< "pg_catalog" namespace
  static constexpr namespace_oid_t NAMESPACE_DEFAULT_NAMESPACE_OID = namespace_oid_t(15);  ///< "public" namespace

 private:
  friend class catalog::CatalogAccessor;
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgConstraintImpl;
  friend class PgCoreImpl;
  friend class PgLanguageImpl;
  friend class PgProcImpl;
  friend class PgTypeImpl;

  friend class noisepage::CatalogTests_CatalogSearchPathTest_Test;

  static constexpr table_oid_t NAMESPACE_TABLE_OID = table_oid_t(11);
  static constexpr index_oid_t NAMESPACE_OID_INDEX_OID = index_oid_t(12);
  static constexpr index_oid_t NAMESPACE_NAME_INDEX_OID = index_oid_t(13);

  /*
   * Column names of the form "NSP[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "NSP_[name]" are
   * noisepage-specific addtions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<namespace_oid_t, uint32_t> NSPOID{col_oid_t{1}};  // INTEGER (pkey)
  static constexpr CatalogColumnDef<storage::VarlenEntry> NSPNAME{col_oid_t{2}};      // VARCHAR

  static constexpr uint8_t NUM_PG_NAMESPACE_COLS = 2;

  static constexpr std::array<col_oid_t, NUM_PG_NAMESPACE_COLS> PG_NAMESPACE_ALL_COL_OIDS{NSPOID.oid_, NSPNAME.oid_};
};

}  // namespace noisepage::catalog::postgres
