#pragma once

#include <array>

#include "catalog/catalog_column_def.h"
#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {
class Builder;
class PgStatisticImpl;

class PgStatistic {
 public:
  static constexpr table_oid_t STATISTIC_TABLE_OID = table_oid_t(91);
  static constexpr index_oid_t STATISTIC_OID_INDEX_OID = index_oid_t(92);

 private:
  friend class Builder;
  friend class PgStatisticImpl;

  static constexpr CatalogColumnDef<table_oid_t, uint32_t> STARELID{col_oid_t{1}};
  static constexpr CatalogColumnDef<col_oid_t, uint32_t> STAATTNUM{col_oid_t{2}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STANULLROWS{col_oid_t{3}};
  static constexpr CatalogColumnDef<uint32_t, uint32_t> STA_NUMROWS{col_oid_t{4}};

  static constexpr uint8_t NUM_PG_STATISTIC_COLS = 4;

  static constexpr std::array<col_oid_t, NUM_PG_STATISTIC_COLS> PG_STATISTIC_ALL_COL_OIDS = {
      STARELID.oid_, STAATTNUM.oid_, STANULLROWS.oid_, STA_NUMROWS.oid_};
};

}  // namespace noisepage::catalog::postgres