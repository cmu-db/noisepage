#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/shared_latch.h"
#include "optimizer/statistics/table_stats.h"

namespace noisepage::optimizer {
/** Hashable type for database and table oid pair */
struct StatsStorageKey {
  StatsStorageKey(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) : db_oid_(db_oid), table_oid_(table_oid) {}
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_oid_;
};
/** Thread safe value for cache */
struct StatsStorageValue {
  explicit StatsStorageValue(std::unique_ptr<TableStats> table_stats) : table_stats_(std::move(table_stats)) {}
  std::unique_ptr<TableStats> table_stats_;
  common::SharedLatch shared_latch_;
};
}  // namespace noisepage::optimizer

namespace std {  // NOLINT
/**
 * template for std::hash of StatsStorageKey
 */
template <>
struct hash<noisepage::optimizer::StatsStorageKey> {
  /**
   * Hashes a StatsStorageKey object.
   * @param stats_storage_key - StatsStorageKey object
   * @return the hash for the StatsStorageKey
   */
  size_t operator()(const noisepage::optimizer::StatsStorageKey &stats_storage_key) const {
    noisepage::common::hash_t hash = noisepage::common::HashUtil::Hash(stats_storage_key.db_oid_);
    hash = noisepage::common::HashUtil::CombineHashes(hash,
                                                      noisepage::common::HashUtil::Hash(stats_storage_key.table_oid_));
    return hash;
  }
};

/**
 * template for std::equal_to of StatsStorageKey
 */
template <>
struct equal_to<noisepage::optimizer::StatsStorageKey> {
  /**
   * Checks for equality between two StatsStorageKey objects
   * @param lhs - StatsStorageKey on left side of equality
   * @param rhs - StatsStorageKey on right side of equality
   * @return whether the StatsStorageKey objects are equal
   */
  bool operator()(const noisepage::optimizer::StatsStorageKey &lhs,
                  const noisepage::optimizer::StatsStorageKey &rhs) const {
    return lhs.db_oid_ == rhs.db_oid_ && lhs.table_oid_ == rhs.table_oid_;
  }
};
}  // namespace std

namespace noisepage::optimizer {
/**
 * Manages all the existing table stats objects. Stores them in an
 * unordered map and keeps track of them using their database and table oids. Can
 * add, update, or delete table stats objects from the storage map.
 *
 * TODO(Joe) Currently there is no cache eviction policy, which means that stats storage will grow forever. If this
 *  becomes an issue we can implement a max number of columns to store with some eviction policy
 */
class StatsStorage {
 public:
  /**
   * Returns a copy of the TableStats object for a specific table
   *
   * Currently all consumers of this method require their own copy of the statistics objects. Instead of them having to
   * create their own copies, we just create a copy for them. This helps simplify thread safety and reduce contention.
   * If a new consumer doesn't need a copy we may want to consider changing this to return a ManagedPointer to prevent
   * unnecessary copying.
   *
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param column_oids - oids of columns
   * @param accessor - catalog accessor
   * @return pointer to a TableStats object
   */
  std::unique_ptr<TableStats> GetTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                            catalog::CatalogAccessor *accessor);

  /**
   * Returns a copy of the ColumnStats object for a specific table and column
   *
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param column_oid - oid of column
   * @param accessor - catalog accessor
   * @return pointer to a ColumnStats object
   */
  std::unique_ptr<ColumnStatsBase> GetColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                  catalog::col_oid_t column_oid, catalog::CatalogAccessor *accessor);

  /**
   * Mark column statistic objects in the cache as having stale information. Next time someone tries to retrieve this
   * column we will get an updated version from the catalog. Columns will get stale whenever someone runs ANALYZE on
   * them
   * @param database_id database oid of database containing the column
   * @param table_id table oid of table containing the column
   * @param col_ids column oids of columns to mark stale
   */
  void MarkStatsStale(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                      const std::vector<catalog::col_oid_t> &col_ids);

 private:
  /**
   * The following tests check to make sure the protected insert/delete functions work.
   */
  FRIEND_TEST(StatsStorageTests, GetTableStatsTest);
  FRIEND_TEST(StatsStorageTests, InsertTableStatsTest);
  FRIEND_TEST(StatsStorageTests, DeleteTableStatsTest);

  /**
   * An unordered map mapping StatsStorageKey objects (database_id and table_id) to
   * TableStats pointers. This represents the storage for TableStats objects.
   */
  std::unordered_map<StatsStorageKey, StatsStorageValue> table_stats_storage_;

  /**
   * latch for inserting into stats storage cache. Prevents one thread from accidentally replacing a TableStats object
   * inserted by another thread for the same table.
   */
  std::mutex insert_latch_;

  /**
   * Returns a pointer the cached TableStats object for a specific table
   *
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param column_oids - oids of columns
   * @param accessor - catalog accessor
   * @return pointer to a TableStats object
   */
  StatsStorageValue &GetStatsStorageValue(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::CatalogAccessor *accessor);


  /**
   * Inserts a TableStats pointer in the table stats storage map.
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param table_stats - TableStats object to be inserted
   */
  void InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                        catalog::CatalogAccessor *accessor);

  /**
   * Update the stale columns of a TableStats
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param accessor - catalog accessor
   */
  static void UpdateStaleColumns(catalog::table_oid_t table_id, StatsStorageValue *stats_storage_value,
                                 catalog::CatalogAccessor *accessor);
};
}  // namespace noisepage::optimizer
