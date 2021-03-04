#include "catalog/postgres/pg_statistic_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::catalog::postgres {

PgStatisticImpl::PgStatisticImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgStatisticImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_statistic_all_oids{PgStatistic::PG_STATISTIC_ALL_COL_OIDS.cbegin(),
                                                     PgStatistic::PG_STATISTIC_ALL_COL_OIDS.end()};
  pg_statistic_all_cols_pri_ = statistics_->InitializerForProjectedRow(pg_statistic_all_oids);
  pg_statistic_all_cols_prm_ = statistics_->ProjectionMapForOids(pg_statistic_all_oids);
  const std::vector<col_oid_t> statistic_oid_index_oids{PgStatistic::STARELID.oid_, PgStatistic::STAATTNUM.oid_};
  statistic_oid_index_pri_ = statistics_->InitializerForProjectedRow(statistic_oid_index_oids);
  statistic_oid_index_prm_ = statistics_->ProjectionMapForOids(statistic_oid_index_oids);
}

void PgStatisticImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                                common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgStatistic::STATISTIC_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      "pg_statistic", Builder::GetStatisticTableSchema(), statistics_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgStatistic::STATISTIC_TABLE_OID,
                      PgStatistic::STATISTIC_OID_INDEX_OID, "pg_statistic_index",
                      Builder::GetStatisticOidIndexSchema(db_oid_), statistic_oid_index_);
}

void PgStatisticImpl::CreateColumnStatistic(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const table_oid_t table_oid, const col_oid_t col_oid,
                                            const Schema::Column &col) {
  auto *const redo = txn->StageWrite(db_oid_, PgStatistic::STATISTIC_TABLE_OID, pg_statistic_all_cols_pri_);
  auto delta = common::ManagedPointer(redo->Delta());
  auto &pm = pg_statistic_all_cols_prm_;

  // Prepare the PR for insertion.
  {
    PgStatistic::STARELID.Set(delta, pm, table_oid);
    PgStatistic::STAATTNUM.Set(delta, pm, col_oid);
    PgStatistic::STA_NONNULLROWS.Set(delta, pm, 0);
    PgStatistic::STA_NUMROWS.Set(delta, pm, 0);
    PgStatistic::STA_DISTINCTROWS.Set(delta, pm, 0);
  }
  const auto tuple_slot = statistics_->Insert(txn, redo);

  const auto oid_pri = statistic_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = statistic_oid_index_->GetKeyOidToOffsetMap();
  byte *const buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Insert into pg_statistic_index.
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    pr->Set<table_oid_t, false>(oid_prm[indexkeycol_oid_t(1)], table_oid, false);
    pr->Set<col_oid_t, false>(oid_prm[indexkeycol_oid_t(2)], col_oid, false);

    bool UNUSED_ATTRIBUTE result = statistic_oid_index_->InsertUnique(txn, *pr, tuple_slot);
    NOISEPAGE_ASSERT(result, "Assigned pg_statistic OIDs failed to be unique.");
  }

  delete[] buffer;
}

bool PgStatisticImpl::DeleteColumnStatistics(const common::ManagedPointer<transaction::TransactionContext> txn,
                                             const table_oid_t table_oid) {
  const auto &oid_pri = statistic_oid_index_->GetProjectedRowInitializer();
  const auto &oid_prm = statistic_oid_index_->GetKeyOidToOffsetMap();

  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  byte *const key_buffer_2 = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Look for the column statistic in pg_statistic_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *pr_lo = oid_pri.InitializeRow(key_buffer);
    auto *pr_hi = oid_pri.InitializeRow(key_buffer_2);

    // Low key (class, min col_oid_t)
    pr_lo->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid, false);
    pr_lo->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::min()),
                                 false);

    // High key (class + 1, max col_oid_t)
    pr_hi->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid + 1, false);
    pr_hi->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::max()),
                                 false);

    statistic_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr_lo, pr_hi, 0, &index_results);
    // TODO(WAN): Is there an assertion that we can make here?
  }

  // Scan pg_statistic to get the columns.
  if (!index_results.empty()) {
    auto pr = common::ManagedPointer(statistic_oid_index_pri_.InitializeRow(key_buffer));
    for (const auto &slot : index_results) {
      auto UNUSED_ATTRIBUTE result = statistics_->Select(txn, slot, pr.Get());
      NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");

      auto &pm = statistic_oid_index_prm_;
      auto *col_oid = PgStatistic::STAATTNUM.Get(pr, pm);
      NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

      // Delete from pg_statistic.
      {
        txn->StageDelete(db_oid_, PgStatistic::STATISTIC_TABLE_OID, slot);
        if (!statistics_->Delete(txn, slot)) {  // Failed to delete some column. Ask to abort.
          delete[] key_buffer;
          delete[] key_buffer_2;
          return false;
        }
      }

      // Delete from pg_statistic_index.
      {
        auto *key_pr = oid_pri.InitializeRow(key_buffer_2);
        key_pr->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid, false);
        key_pr->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), *col_oid, false);
        statistic_oid_index_->Delete(txn, *key_pr, slot);
      }
    }
  }

  delete[] key_buffer;
  delete[] key_buffer_2;
  return true;
}

}  // namespace noisepage::catalog::postgres
