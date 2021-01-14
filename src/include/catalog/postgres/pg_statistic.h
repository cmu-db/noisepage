#pragma once

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgCoreImpl;

class PgStatistic {
 public:
  static constexpr table_oid_t STATISTIC_TABLE_OID = table_oid_t(91);
  static constexpr index_oid_t STATISTIC_OID_INDEX_OID = index_oid_t(92);

 private:
  friend class Builder;
  friend class PgCoreImpl;

  /*
   * Column names of the form "STA[name]_COL_OID" are present in the PostgreSQL
   * catalog specification and columns of the form "STA_[name]_COL_OID" are
   * terrier-specific additions (generally pointers to internal objects).
   */
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> STARELID_COL_OID{col_oid_t{1}};
  static constexpr CatalogColumnDef<col_oid_t , uint32_t> STAATTNUM_COL_OID{col_oid_t{2}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STANULLROWS_COL_OID{col_oid_t{3}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STA_NUMROWS_COL_OID{col_oid_t{4}};

  static constexpr uint8_t NUM_PG_STATISTIC_COLS = 4;

  static constexpr std::array<col_oid_t, NUM_PG_STATISTIC_COLS> PG_STATISTIC_ALL_COL_OIDS = {
      STARELID_COL_OID.oid_, STAATTNUM_COL_OID.oid_, STANULLROWS_COL_OID.oid_, STA_NUMROWS_COL_OID.oid_};
};

}  // namespace terrier::catalog::postgres