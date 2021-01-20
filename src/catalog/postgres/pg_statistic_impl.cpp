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

  const std::vector<col_oid_t> delete_statistics_oids{PgStatistic::STAATTNUM.oid_};
  delete_statistics_pri_ = statistics_->InitializerForProjectedRow(delete_statistics_oids);
  delete_statistics_prm_ = statistics_->ProjectionMapForOids(delete_statistics_oids);
}

void PgStatisticImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                                common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgStatistic::STATISTIC_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      "pg_statistic", Builder::GetStatisticTableSchema(), statistics_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgStatistic::STATISTIC_TABLE_OID,
                      PgStatistic::STATISTIC_OID_INDEX_OID, "pg_statistic_index",
                      Builder::GetStatisticOidIndexSchema(db_oid_), statistics_oid_index_);
}

template <typename Column, typename ClassOid, typename ColOid>
void PgStatisticImpl::CreateColumnStatistic(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  auto *const redo = txn->StageWrite(db_oid_, PgStatistic::STATISTIC_TABLE_OID, pg_statistic_all_cols_pri_);
  auto delta = common::ManagedPointer(redo->Delta());
  auto &pm = pg_statistic_all_cols_prm_;

  // Prepare the PR for insertion.
  {
    PgStatistic::STARELID.Set(delta, pm, class_oid);
    PgStatistic::STAATTNUM.Set(delta, pm, col_oid);
    PgStatistic::STA_NULLROWS.Set(delta, pm, 0);
    PgStatistic::STA_NUMROWS.Set(delta, pm, 0);
  }
  const auto tuple_slot = statistics_->Insert(txn, redo);

  const auto oid_pri = statistics_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = statistics_oid_index_->GetKeyOidToOffsetMap();
  byte *const buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Insert into pg_statistic_index.
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    pr->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], class_oid, false);
    pr->Set<ColOid, false>(oid_prm[indexkeycol_oid_t(2)], col_oid, false);

    bool UNUSED_ATTRIBUTE result = statistics_oid_index_->InsertUnique(txn, *pr, tuple_slot);
    NOISEPAGE_ASSERT(result, "Assigned pg_statistic OIDs failed to be unique.");
  }

  delete[] buffer;
}

template <typename Column, typename ClassOid>
bool PgStatisticImpl::DeleteColumnStatistics(const common::ManagedPointer<transaction::TransactionContext> txn,
                                             const ClassOid class_oid) {
  const auto &oid_pri = statistics_oid_index_->GetProjectedRowInitializer();
  const auto &oid_prm = statistics_oid_index_->GetKeyOidToOffsetMap();

  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_statistics_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Look for the column statistic in pg_statistic_index.
  std::vector<storage::TupleSlot> index_results;
  {
    NOISEPAGE_ASSERT(delete_statistics_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                     "Buffer must be large enough to fit largest PR.");
    auto *pr_lo = oid_pri.InitializeRow(buffer);
    auto *pr_hi = oid_pri.InitializeRow(key_buffer);

    // Low key (class, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    pr_lo->Set<ClassOid, false>(oid_prm.at(indexkeycol_oid_t(1)), class_oid, false);
    pr_lo->Set<uint32_t, false>(oid_prm.at(indexkeycol_oid_t(2)), 0, false);

    // High key (class + 1, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    pr_hi->Set<ClassOid, false>(oid_prm.at(indexkeycol_oid_t(1)), ClassOid(class_oid + 1), false);
    pr_hi->Set<uint32_t, false>(oid_prm.at(indexkeycol_oid_t(2)), 0, false);

    statistics_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr_lo, pr_hi, 0, &index_results);
    NOISEPAGE_ASSERT(
        !index_results.empty(),
        "Incorrect number of results from index scan. empty() implies that function was called with an oid "
        "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");
  }

  // Scan pg_statistic to get the columns.
  {
    auto pr = common::ManagedPointer(delete_statistics_pri_.InitializeRow(buffer));
    for (const auto &slot : index_results) {
      auto UNUSED_ATTRIBUTE result = statistics_->Select(txn, slot, pr.Get());
      NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");

      auto &pm = delete_statistics_prm_;
      auto *col_oid = PgStatistic::STAATTNUM.Get(pr, pm);
      NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

      // Delete from pg_statistic.
      {
        txn->StageDelete(db_oid_, PgStatistic::STATISTIC_TABLE_OID, slot);
        result = statistics_->Delete(txn, slot);
        if (!result) {  // Failed to delete some column. Ask to abort.
          delete[] buffer;
          delete[] key_buffer;
          return false;
        }
      }

      // Delete from pg_statistic_index.
      {
        auto *key_pr = oid_pri.InitializeRow(key_buffer);
        key_pr->Set<ClassOid, false>(oid_prm.at(indexkeycol_oid_t(1)), class_oid, false);
        key_pr->Set<uint32_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid->UnderlyingValue(), false);
        statistics_oid_index_->Delete(txn, *key_pr, slot);
      }
    }
  }

  delete[] buffer;
  delete[] key_buffer;
  return true;
}

}  // namespace noisepage::catalog::postgres