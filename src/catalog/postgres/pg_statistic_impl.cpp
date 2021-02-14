#include "catalog/postgres/pg_statistic_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "optimizer/statistics/new_column_stats.h"
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
    PgStatistic::STA_NULLROWS.Set(delta, pm, 0);
    PgStatistic::STA_NUMROWS.Set(delta, pm, 0);
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

  auto null_rows = *PgStatistic::STA_NULLROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  auto num_rows = *PgStatistic::STA_NUMROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
  double frac_null = static_cast<double>(null_rows) / static_cast<double>(num_rows);

  delete[] key_buffer;

  auto type = database_catalog->GetSchema(txn, table_oid).GetColumn(col_oid).Type();

  // TODO(Joe) come up with better way for this
  switch (type) {
    case type::TypeId::BOOLEAN: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::BoolVal;
      using CppType = decltype(T::val_);
      auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            std::move(top_k), std::move(histogram), type);
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::Integer;
      using CppType = decltype(T::val_);
      auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            std::move(top_k), std::move(histogram), type);
    }
    case type::TypeId::REAL: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::Real;
      using CppType = decltype(T::val_);
      auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            std::move(top_k), std::move(histogram), type);
    }
    // TODO(Joe) Uncomment this when you have hash shit from ANALYZE PR
    /*case type::TypeId::DECIMAL: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::DecimalVal;
      using CppType = decltype(T::val_);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            nullptr, std::move(histogram), type);
    }
    case type::TypeId::TIMESTAMP: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::TimestampVal;
      using CppType = decltype(T::val_);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            nullptr, std::move(histogram), type);
    }
    case type::TypeId::DATE: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::DateVal;
      using CppType = decltype(T::val_);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            nullptr, std::move(histogram), type);
    }*/
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      // TODO(Joe) we'll have to fix this with deserialization
      using T = execution::sql::StringVal;
      using CppType = decltype(T::val_);
      auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
      auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
      return std::make_unique<optimizer::NewColumnStats<T>>(db_oid_, table_oid, col_oid, num_rows, frac_null,
                                                            std::move(top_k), std::move(histogram), type);
    }
    default:
      // TODO(Joe) fix error message
      UNREACHABLE("Invalid type");
  }
}

std::unique_ptr<optimizer::TableStats> PgStatisticImpl::GetTableStatistics(
    common::ManagedPointer<transaction::TransactionContext> txn,
    common::ManagedPointer<DatabaseCatalog> database_catalog, table_oid_t table_oid) {
  // TODO(Joe) remove duplication from delete
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
  NOISEPAGE_ASSERT(!index_results.empty(), "Every table should have column stats");

  std::vector<std::unique_ptr<optimizer::ColumnStatsBase>> col_stats_list;

  auto all_cols_pr = common::ManagedPointer(pg_statistic_all_cols_pri_.InitializeRow(key_buffer));
  for (const auto &slot : index_results) {
    statistics_->Select(txn, slot, all_cols_pr.Get());

    auto col_oid = *PgStatistic::STAATTNUM.Get(all_cols_pr, pg_statistic_all_cols_prm_);
    auto null_rows = *PgStatistic::STA_NULLROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
    auto num_rows = *PgStatistic::STA_NUMROWS.Get(all_cols_pr, pg_statistic_all_cols_prm_);
    // TODO(Joe) replace these with real values
    auto type = database_catalog->GetSchema(txn, table_oid).GetColumn(col_oid).Type();

    // TODO(Joe) come up with better way for this
    switch (type) {
      case type::TypeId::BOOLEAN: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::BoolVal;
        using CppType = decltype(T::val_);
        auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, std::move(top_k), std::move(histogram), type));
        break;
      }
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::Integer;
        using CppType = decltype(T::val_);
        auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, std::move(top_k), std::move(histogram), type));
        break;
      }
      case type::TypeId::REAL: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::Real;
        using CppType = decltype(T::val_);
        auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, std::move(top_k), std::move(histogram), type));
        break;
      }
      /*case type::TypeId::DECIMAL: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::DecimalVal;
        using CppType = decltype(T::val_);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, nullptr, std::move(histogram), type));
        break;
      }
      case type::TypeId::TIMESTAMP: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::TimestampVal;
        using CppType = decltype(T::val_);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, nullptr, std::move(histogram), type));
        break;
      }
      case type::TypeId::DATE: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::DateVal;
        using CppType = decltype(T::val_);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, nullptr, std::move(histogram), type));
        break;
      }*/
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        // TODO(Joe) we'll have to fix this with deserialization
        using T = execution::sql::StringVal;
        using CppType = decltype(T::val_);
        auto top_k = std::make_unique<optimizer::TopKElements<CppType>>(4, 5);
        auto histogram = std::make_unique<optimizer::Histogram<CppType>>(666);
        col_stats_list.push_back(std::make_unique<optimizer::NewColumnStats<T>>(
            db_oid_, table_oid, col_oid, num_rows, null_rows, std::move(top_k), std::move(histogram), type));
        break;
      }
      default:
        // TODO(Joe) fix error message
        UNREACHABLE("Invalid type");
    }
  }

  delete[] key_buffer;
  delete[] key_buffer_2;

  // TODO(Joe) fix num rows
  return std::make_unique<optimizer::TableStats>(db_oid_, table_oid, 666, &col_stats_list);
}

}  // namespace noisepage::catalog::postgres
