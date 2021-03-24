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
  /**
   * Constructor
   * @param db_oid database oid
   * @param table_oid table oid
   */
  StatsStorageKey(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) : db_oid_(db_oid), table_oid_(table_oid) {}
  /** database oid */
  catalog::db_oid_t db_oid_;
  /** table oid */
  catalog::table_oid_t table_oid_;
};
/** Thread safe value for cache */
struct StatsStorageValue {
  /**
   * Constructor
   * @param table_stats Table Statistics
   */
  explicit StatsStorageValue(TableStats table_stats) : table_stats_(std::move(table_stats)) {}
  /** Table Statistics */
  TableStats table_stats_;
  /** Shared Latch for Table Statistics */
  common::SharedLatch shared_latch_;
};

/** Thread safe value to return back to consumers of cache */
struct StatsStorageReference {
  /**
   * Constructor
   * @param stats_storage_value pointer to stats_storage_value
   * @param stats_storage_shared_latch acquired read latch on stats storage
   */
  explicit StatsStorageReference(StatsStorageValue *stats_storage_value,
                                 common::SharedLatch::UniqueSharedLatch stats_storage_shared_latch)
      : stats_storage_value_(stats_storage_value), stats_storage_shared_latch_(std::move(stats_storage_shared_latch)) {}
  /** Stats Storage Value */
  StatsStorageValue *stats_storage_value_;
  /** Unique Shared Latch on Stats Storage */
  common::SharedLatch::UniqueSharedLatch stats_storage_shared_latch_;
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
 * TODO(Joe Koshakow) Currently there is no cache eviction policy, which means that stats storage will grow forever. If
 *  this becomes an issue we can implement a max number of columns to store with some eviction policy
 */
class StatsStorage {
 public:
  /**
   * Returns a reference of a TableStats object for a specific table, and an acquired read lock on the entire Stats
   * Storage
   *
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param accessor - catalog accessor
   * @return reference to a TableStats object, latch for that TableStats object, and an acquired shared latch on
   * StatsStorage
   */
  StatsStorageReference GetTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                      catalog::CatalogAccessor *accessor);

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
   * An unordered map mapping StatsStorageKey objects (database_id and table_id) to
   * TableStats pointers. This represents the storage for TableStats objects.
   */
  std::unordered_map<StatsStorageKey, StatsStorageValue> table_stats_storage_;

  /**
   * latch for reading and modifying table_stats_storage_.
   */
  common::SharedLatch stats_storage_latch_;

  /**
   * Checks with StatsStorage contains stats for a certain table
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @return true if StatsStorage contains stats for specified table, false otherwise
   */
  bool ContainsTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id);

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
