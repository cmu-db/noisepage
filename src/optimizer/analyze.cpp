#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "optimizer/analyze.h"

namespace terrier {

template <typename T>
terrier::optimizer::ColumnStats CalculateColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     catalog::col_oid_t columnID, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber) {
  // Stats collecting temporary data-structures
  std::unordered_map<T, unsigned> counting_map;
  unsigned num_null_values = 0;
  unsigned num_rows = 0;
  unsigned top_k = mostFrequentNumber;
  std::vector<double> histogram_bounds;
  histogram_bounds.push_back(DBL_MAX);
  histogram_bounds.push_back(DBL_MIN);

  std::vector<catalog::col_oid_t> current_col_oid_vector{columnID};
  storage::ProjectionMap projection_list_indices = sql_table->ProjectionMapForOids(current_col_oid_vector);
  auto initializer =
      sql_table->InitializerForProjectedColumns(current_col_oid_vector, common::Constants::K_DEFAULT_VECTOR_SIZE);
  auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
  storage::ProjectedColumns *projected_column = initializer.Initialize(buffer);

  // scan the results
  auto iterator_table = sql_table->begin();
  while (iterator_table != sql_table->end()) {
    sql_table->Scan(scan_txn, &iterator_table, projected_column);
    common::RawBitmap *null_bit_map = projected_column->ColumnNullBitmap(projection_list_indices[columnID]);
    T *col_pointer = reinterpret_cast<T *>(projected_column->ColumnStart(projection_list_indices[columnID]));
    for (uint32_t i = 0; i < projected_column->NumTuples(); i++) {
      if (null_bit_map->Test(i)) {
        if (counting_map.find(col_pointer[i]) != counting_map.end())
          counting_map[col_pointer[i]]++;
        else
          counting_map[col_pointer[i]] = 1;

        histogram_bounds[0] = std::min(static_cast<double>(col_pointer[i]), histogram_bounds[0]);
        histogram_bounds[1] = std::max(static_cast<double>(col_pointer[i]), histogram_bounds[1]);
      } else {
        num_null_values += 1;
      }
      num_rows += 1;
    }
  }

  std::vector<std::pair<double, double>> intermediate_hist;
  intermediate_hist.reserve(counting_map.size());
  for (auto it : counting_map) {
    intermediate_hist.emplace_back(std::make_pair(static_cast<double>(it.second), static_cast<double>(it.first)));
  }

  std::sort(intermediate_hist.begin(), intermediate_hist.end());
  std::reverse(intermediate_hist.begin(), intermediate_hist.end());

  std::vector<double> most_common_values;
  std::vector<double> most_common_frequencies;

  unsigned index = 0;
  while (top_k--) {
    most_common_values.push_back(intermediate_hist[index].second);
    most_common_frequencies.push_back(intermediate_hist[index].first);
    index++;
  }

  terrier::optimizer::ColumnStats col_stats(database_id, table_id, columnID, num_rows, counting_map.size(),
                                            static_cast<double>(num_null_values) / static_cast<double>(num_rows),
                                            most_common_values, most_common_frequencies, histogram_bounds, true);

  delete[] buffer;
  return col_stats;
}

std::vector<terrier::optimizer::ColumnStats> Analyze(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     const catalog::Schema &table_schema, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber) {
  std::vector<catalog::col_oid_t> col_oids;
  col_oids.reserve(table_schema.GetColumns().size());
  for (const auto &col : table_schema.GetColumns()) {
    col_oids.push_back(col.Oid());
  }

  std::vector<terrier::optimizer::ColumnStats> col_stats_vector;

  for (auto col_id_iterator : col_oids) {
    switch (table_schema.GetColumn(col_id_iterator).Type()) {
      case static_cast<terrier::type::TypeId>(0): {
        break;
      }
      case static_cast<terrier::type::TypeId>(1): {
        col_stats_vector.emplace_back(CalculateColumnStats<bool>(database_id, table_id, col_id_iterator, sql_table,
                                                                 scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(2): {
        col_stats_vector.emplace_back(CalculateColumnStats<int8_t>(database_id, table_id, col_id_iterator, sql_table,
                                                                   scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(3): {
        col_stats_vector.emplace_back(CalculateColumnStats<int16_t>(database_id, table_id, col_id_iterator, sql_table,
                                                                    scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(4): {
        col_stats_vector.emplace_back(CalculateColumnStats<int32_t>(database_id, table_id, col_id_iterator, sql_table,
                                                                    scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(5): {
        col_stats_vector.emplace_back(CalculateColumnStats<int64_t>(database_id, table_id, col_id_iterator, sql_table,
                                                                    scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(6): {
        col_stats_vector.emplace_back(CalculateColumnStats<uint64_t>(database_id, table_id, col_id_iterator, sql_table,
                                                                     scan_txn, mostFrequentNumber));
        break;
      }
      case static_cast<terrier::type::TypeId>(7): {
        col_stats_vector.emplace_back(CalculateColumnStats<double>(database_id, table_id, col_id_iterator, sql_table,
                                                                   scan_txn, mostFrequentNumber));
        break;
      }
      default:
        break;
    }
  }

  return col_stats_vector;
}
}  // namespace terrier
