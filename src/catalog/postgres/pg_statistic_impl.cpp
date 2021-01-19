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

void PgStatisticImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                                common::ManagedPointer<DatabaseCatalog> dbc) {
  // pg_statistic and associated indexes.
  dbc->BootstrapTable(txn, PgStatistic::STATISTIC_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      "pg_statistic", Builder::GetStatisticTableSchema(), statistics_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgStatistic::STATISTIC_TABLE_OID,
                      PgStatistic::STATISTIC_OID_INDEX_OID, "pg_statistic_index",
                      Builder::GetStatisticOidIndexSchema(db_oid_), statistics_oid_index_);
}

void PgStatisticImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_statistic_all_oids{PgStatistic::PG_STATISTIC_ALL_COL_OIDS.cbegin(),
                                                     PgStatistic::PG_STATISTIC_ALL_COL_OIDS.end()};
  pg_statistic_all_cols_pri_ = statistics_->InitializerForProjectedRow(pg_statistic_all_oids);
  pg_statistic_all_cols_prm_ = statistics_->ProjectionMapForOids(pg_statistic_all_oids);

  // Used to select rows to delete in DeleteColumnStatistics
  const std::vector<col_oid_t> delete_statistics_oids{PgStatistic::STAATTNUM.oid_};
  delete_statistics_pri_ = statistics_->InitializerForProjectedRow(delete_statistics_oids);
  delete_statistics_prm_ = statistics_->ProjectionMapForOids(delete_statistics_oids);
}

template <typename Column, typename ClassOid, typename ColOid>
void PgStatisticImpl::CreateColumnStatistic(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  // Step 1: Insert into the table
  auto *const redo = txn->StageWrite(db_oid_, PgStatistic::STATISTIC_TABLE_OID, pg_statistic_all_cols_pri_);
  auto delta = common::ManagedPointer(redo->Delta());
  auto &pm = pg_statistic_all_cols_prm_;

  // Insert into pg_statistic.
  {
    PgStatistic::STARELID.Set(delta, pm, class_oid);
    PgStatistic::STAATTNUM.Set(delta, pm, col_oid);
    PgStatistic::STANULLROWS.Set(delta, pm, 0);
    PgStatistic::STA_NUMROWS.Set(delta, pm, 0);
  }

  // Finally, insert into the table to get the tuple slot
  const auto tupleslot = statistics_->Insert(txn, redo);

  // Step 2: Insert into oid index
  const auto oid_pri = statistics_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = statistics_oid_index_->GetKeyOidToOffsetMap();
  // Create a buffer large enough for all columns
  auto const buffer = std::unique_ptr<byte[]>(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));
  auto *pr = oid_pri.InitializeRow(buffer.get());
  // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
  // Builder::GetStatisticOidIndexSchema()
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<ColOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = col_oid;

  bool UNUSED_ATTRIBUTE result = statistics_oid_index_->InsertUnique(txn, *pr, tupleslot);
  NOISEPAGE_ASSERT(result, "Assigned OIDs failed to be unique.");
}

template <typename Column, typename ClassOid>
bool PgStatisticImpl::DeleteColumnStatistics(const common::ManagedPointer<transaction::TransactionContext> txn,
                                             const ClassOid class_oid) {
  const auto &oid_pri = statistics_oid_index_->GetProjectedRowInitializer();
  const auto &oid_prm = statistics_oid_index_->GetKeyOidToOffsetMap();

  // Step 1: Read Index
  std::vector<storage::TupleSlot> index_results;
  {
    // Buffer is large enough to hold all prs
    const std::unique_ptr<byte[]> buffer_lo(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));
    const std::unique_ptr<byte[]> buffer_hi(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));

    // Scan the class index
    auto *pr_lo = oid_pri.InitializeRow(buffer_lo.get());
    auto *pr_hi = oid_pri.InitializeRow(buffer_hi.get());

    // Write the attributes in the ProjectedRow
    // Low key (class, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    pr_lo->Set<ClassOid, false>(oid_prm.at(indexkeycol_oid_t(1)), class_oid, false);
    pr_lo->Set<uint32_t, false>(oid_prm.at(indexkeycol_oid_t(2)), 0, false);

    // High key (class + 1, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    pr_hi->Set<ClassOid, false>(oid_prm.at(indexkeycol_oid_t(1)), ClassOid(class_oid + 1), false);
    pr_hi->Set<uint32_t, false>(oid_prm.at(indexkeycol_oid_t(2)), 0, false);

    statistics_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr_lo, pr_hi, 0, &index_results);
  }

  NOISEPAGE_ASSERT(!index_results.empty(),
                   "Incorrect number of results from index scan. empty() implies that function was called with an oid "
                   "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Step 2: Scan the table to get the columns
  const std::unique_ptr<byte[]> buffer(
      common::AllocationUtil::AllocateAligned(delete_statistics_pri_.ProjectedRowSize()));
  const std::unique_ptr<byte[]> key_buffer(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));

  auto *pr = delete_statistics_pri_.InitializeRow(buffer.get());
  for (const auto &slot : index_results) {
    // 1. Extract attributes from the tuple for the index deletions
    auto UNUSED_ATTRIBUTE result = statistics_->Select(txn, slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    const auto *const col_oid = reinterpret_cast<const uint32_t *const>(
        pr->AccessWithNullCheck(delete_statistics_prm_.at(PgStatistic::STAATTNUM.oid_)));
    NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

    // 2. Delete from the table
    txn->StageDelete(db_oid_, PgStatistic::STATISTIC_TABLE_OID, slot);
    result = statistics_->Delete(txn, slot);
    if (!result) {
      // Failed to delete one of the columns, return false to indicate failure
      return false;
    }

    // 3. Delete from oid index
    auto *key_pr = oid_pri.InitializeRow(key_buffer.get());
    // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
    // Builder::GetStatisticOidIndexSchema()
    *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(oid_prm.at(indexkeycol_oid_t(1))))) = class_oid;
    *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(oid_prm.at(indexkeycol_oid_t(2))))) = *col_oid;
    statistics_oid_index_->Delete(txn, *key_pr, slot);
  }

  return true;
}

}  // namespace noisepage::catalog::postgres