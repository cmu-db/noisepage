#include "catalog/postgres/pg_statistic_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "optimizer/statistics/column_stats.h"
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
                      PG_STATISTIC_TABLE_NAME, Builder::GetStatisticTableSchema(), statistics_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgStatistic::STATISTIC_TABLE_OID,
                      PgStatistic::STATISTIC_OID_INDEX_OID, PG_STATISTIC_INDEX_NAME,
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

    // Low key (table_oid_t, min col_oid_t)
    pr_lo->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid, false);
    pr_lo->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::min()),
                                 false);

    // High key (table_oid_t + 1, min col_oid_t)
    pr_hi->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid + 1, false);
    pr_hi->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::min()),
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

std::unique_ptr<optimizer::ColumnStatsBase> PgStatisticImpl::GetColumnStatistics(
    common::ManagedPointer<transaction::TransactionContext> txn,
    common::ManagedPointer<DatabaseCatalog> database_catalog, table_oid_t table_oid, col_oid_t col_oid) {
  const auto &oid_pri = statistic_oid_index_->GetProjectedRowInitializer();
  const auto &oid_prm = statistic_oid_index_->GetKeyOidToOffsetMap();

  auto *const key_buffer = common::AllocationUtil::AllocateAligned(pg_statistic_all_cols_pri_.ProjectedRowSize());

  auto *stats_pr = oid_pri.InitializeRow(key_buffer);

  std::vector<storage::TupleSlot> results;
  stats_pr->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid, false);
  stats_pr->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid, false);

  statistic_oid_index_->ScanKey(*txn, *stats_pr, &results);
  NOISEPAGE_ASSERT(!results.empty() && results.size() == 1, "Every column should have a single row in pg_statistic");

  auto all_cols_pr = common::ManagedPointer(pg_statistic_all_cols_pri_.InitializeRow(key_buffer));
  auto found_tuple_slot = results[0];
  statistics_->Select(txn, found_tuple_slot, all_cols_pr.Get());

  std::unique_ptr<optimizer::ColumnStatsBase> column_stats;
  auto type = database_catalog->GetSchema(txn, table_oid).GetColumn(col_oid).Type();
  column_stats = CreateColumnStats(all_cols_pr, table_oid, col_oid, type);

  delete[] key_buffer;
  return column_stats;
}

optimizer::TableStats PgStatisticImpl::GetTableStatistics(common::ManagedPointer<transaction::TransactionContext> txn,
                                                          common::ManagedPointer<DatabaseCatalog> database_catalog,
                                                          table_oid_t table_oid) {
  const auto &oid_pri = statistic_oid_index_->GetProjectedRowInitializer();
  const auto &oid_prm = statistic_oid_index_->GetKeyOidToOffsetMap();

  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_statistic_all_cols_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Look for the column statistic in pg_statistic_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    auto *pr_hi = oid_pri.InitializeRow(key_buffer);

    // Low key (table_oid_t, min col_oid_t)
    pr->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid, false);
    pr->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::min()), false);

    // High key (table_oid_t + 1, max col_oid_t)
    pr_hi->Set<table_oid_t, false>(oid_prm.at(indexkeycol_oid_t(1)), table_oid + 1, false);
    pr_hi->Set<col_oid_t, false>(oid_prm.at(indexkeycol_oid_t(2)), col_oid_t(std::numeric_limits<uint32_t>::min()),
                                 false);

    statistic_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, pr_hi, 0, &index_results);
  }
  NOISEPAGE_ASSERT(!index_results.empty(), "Every table should have column stats");

  std::vector<std::unique_ptr<optimizer::ColumnStatsBase>> col_stats_list;

  auto all_cols_pr = common::ManagedPointer(pg_statistic_all_cols_pri_.InitializeRow(buffer));
  for (const auto &slot : index_results) {
    statistics_->Select(txn, slot, all_cols_pr.Get());

    auto col_oid = *PgStatistic::STAATTNUM.Get(all_cols_pr, pg_statistic_all_cols_prm_);
    auto type = database_catalog->GetSchema(txn, table_oid).GetColumn(col_oid).Type();
    col_stats_list.emplace_back(CreateColumnStats(all_cols_pr, table_oid, col_oid, type));
  }
  delete[] buffer;
  delete[] key_buffer;
  return optimizer::TableStats(db_oid_, table_oid, &col_stats_list);
}

std::unique_ptr<optimizer::ColumnStatsBase> PgStatisticImpl::CreateColumnStats(
    common::ManagedPointer<storage::ProjectedRow> all_cols_pr, table_oid_t table_oid, col_oid_t col_oid,
    type::TypeId type) {
  auto num_rows = *PgStatistic::STA_NUMROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  auto non_null_rows = *PgStatistic::STA_NONNULLROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  auto distinct_values = *PgStatistic::STA_DISTINCTROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  const auto *top_k_str = PgStatistic::STA_TOPK.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  const auto *histogram_str = PgStatistic::STA_HISTOGRAM.Get(all_cols_pr, pg_statistic_all_cols_prm_);

  switch (type) {
    case type::TypeId::BOOLEAN:
      return CreateColumnStats<execution::sql::BoolVal>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                        top_k_str, histogram_str, type);
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT:
      return CreateColumnStats<execution::sql::Integer>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                        top_k_str, histogram_str, type);
    case type::TypeId::REAL:
      return CreateColumnStats<execution::sql::Real>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                     top_k_str, histogram_str, type);
    case type::TypeId::DECIMAL:
      return CreateColumnStats<execution::sql::DecimalVal>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                           top_k_str, histogram_str, type);
    case type::TypeId::TIMESTAMP:
      return CreateColumnStats<execution::sql::TimestampVal>(table_oid, col_oid, num_rows, non_null_rows,
                                                             distinct_values, top_k_str, histogram_str, type);
    case type::TypeId::DATE:
      return CreateColumnStats<execution::sql::DateVal>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                        top_k_str, histogram_str, type);
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      return CreateColumnStats<execution::sql::StringVal>(table_oid, col_oid, num_rows, non_null_rows, distinct_values,
                                                          top_k_str, histogram_str, type);
    default:
      UNREACHABLE("Invalid column type");
  }
}

template <typename T>
std::unique_ptr<optimizer::ColumnStatsBase> PgStatisticImpl::CreateColumnStats(
    table_oid_t table_oid, col_oid_t col_oid, size_t num_rows, size_t non_null_rows, size_t distinct_values,
    const storage::VarlenEntry *top_k_str, const storage::VarlenEntry *histogram_str, type::TypeId type) {
  using CppType = decltype(T::val_);
  // top_k and histogram will be NULL on a newly created table
  auto top_k = top_k_str != nullptr
                   ? optimizer::TopKElements<CppType>::Deserialize(top_k_str->Content(), top_k_str->Size())
                   : optimizer::TopKElements<CppType>();
  auto histogram = histogram_str != nullptr
                       ? optimizer::Histogram<CppType>::Deserialize(histogram_str->Content(), histogram_str->Size())
                       : optimizer::Histogram<CppType>();
  return std::make_unique<optimizer::ColumnStats<T>>(
      db_oid_, table_oid, col_oid, num_rows, non_null_rows, distinct_values,
      std::make_unique<optimizer::TopKElements<CppType>>(std::move(top_k)),
      std::make_unique<optimizer::Histogram<CppType>>(std::move(histogram)), type);
}

}  // namespace noisepage::catalog::postgres
