#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::catalog::postgres {
class Builder;
class PgStatisticImpl;

/** The OIDs used by the NoisePage version of pg_statistic. */
class PgStatistic {
 private:
  friend class storage::RecoveryManager;
  friend class Builder;
  friend class PgStatisticImpl;

  static constexpr table_oid_t STATISTIC_TABLE_OID = table_oid_t(91);
  static constexpr index_oid_t STATISTIC_OID_INDEX_OID = index_oid_t(92);

  /*
   * Column names of the form "STA[name]" are present in the PostgreSQL
   * catalog specification and columns of the form "STA_[name]" are
   * noisepage-specific additions.
   */
  static constexpr CatalogColumnDef<table_oid_t, uint32_t> STARELID{col_oid_t{1}};
  static constexpr CatalogColumnDef<col_oid_t, uint32_t> STAATTNUM{col_oid_t{2}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STA_NULLROWS{col_oid_t{3}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STA_NUMROWS{col_oid_t{4}};

  static constexpr uint8_t NUM_PG_STATISTIC_COLS = 4;

  static constexpr std::array<col_oid_t, NUM_PG_STATISTIC_COLS> PG_STATISTIC_ALL_COL_OIDS = {
      STARELID.oid_, STAATTNUM.oid_, STA_NULLROWS.oid_, STA_NUMROWS.oid_};
};

}  // namespace noisepage::catalog::postgres
