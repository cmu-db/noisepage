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
struct TableStatsKey {
  /**
   * Constructor
   * @param db_oid database oid
   * @param table_oid table oid
   */
  TableStatsKey(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) : db_oid_(db_oid), table_oid_(table_oid) {}
  /** database oid */
  catalog::db_oid_t db_oid_;
  /** table oid */
  catalog::table_oid_t table_oid_;
};
/** Thread safe value for cache */
struct TableStatsValue {
  /**
   * Constructor
   * @param table_stats Table Statistics
   */
  explicit TableStatsValue(TableStats table_stats) : table_stats_(std::move(table_stats)) {}
  /** Table Statistics */
  TableStats table_stats_;
  /** Shared Latch for Table Statistics */
  common::SharedLatch shared_latch_;
};

/** Thread safe value to return back to consumers of cache */
struct LatchedTableStatsReference {
  /**
   * Constructor
   * @param table_stats reference to the table stats
   * @param table_stats_shared_latch acquired read latch on the table stats
   * @param stats_storage_shared_latch acquired read latch on stats storage
   */
  explicit LatchedTableStatsReference(const TableStats &table_stats, common::SharedLatchGuard table_stats_shared_latch,
                                      common::SharedLatchGuard stats_storage_shared_latch)
      : table_stats_(table_stats),
        table_stats_shared_latch_(std::move(table_stats_shared_latch)),
        stats_storage_shared_latch_(std::move(stats_storage_shared_latch)) {}
  /** Table Statistics */
  const TableStats &table_stats_;
  /** Acquired Shared Latch on Table Stats */
  common::SharedLatchGuard table_stats_shared_latch_;
  /** Acquired Shared Latch on Stats Storage */
  common::SharedLatchGuard stats_storage_shared_latch_;
};
}  // namespace noisepage::optimizer

namespace std {  // NOLINT
/**
 * template for std::hash of TableStatsKey
 */
template <>
struct hash<noisepage::optimizer::TableStatsKey> {
  /**
   * Hashes a TableStatsKey object.
   * @param table_stats_key - TableStatsKey object
   * @return the hash for the TableStatsKey
   */
  size_t operator()(const noisepage::optimizer::TableStatsKey &table_stats_key) const {
    noisepage::common::hash_t hash = noisepage::common::HashUtil::Hash(table_stats_key.db_oid_);
    hash =
        noisepage::common::HashUtil::CombineHashes(hash, noisepage::common::HashUtil::Hash(table_stats_key.table_oid_));
    return hash;
  }
};

/**
 * template for std::equal_to of TableStatsKey
 */
template <>
struct equal_to<noisepage::optimizer::TableStatsKey> {
  /**
   * Checks for equality between two TableStatsKey objects
   * @param lhs - TableStatsKey on left side of equality
   * @param rhs - TableStatsKey on right side of equality
   * @return whether the TableStatsKey objects are equal
   */
  bool operator()(const noisepage::optimizer::TableStatsKey &lhs,
                  const noisepage::optimizer::TableStatsKey &rhs) const {
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
   * Returns a reference of a TableStats object for a specific table, an acquired shared latch for that TableStats
   * object, and an acquired shared latch on the entire Stats Storage. No locking or unlocking is needed from the
   * consumer, both latches are already acquired and will automatically unlock when they fall out of scope.
   *
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @param accessor - catalog accessor
   * @return reference to a TableStats object, an acquired shared latch for that TableStats object, and an acquired
   * shared latch on StatsStorage
   */
  LatchedTableStatsReference GetTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
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
   * An unordered map mapping TableStatsKey objects (database_id and table_id) to
   * TableStats pointers. This represents the storage for TableStats objects.
   */
  std::unordered_map<TableStatsKey, TableStatsValue> table_stats_storage_;

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
  static void UpdateStaleColumns(catalog::table_oid_t table_id, TableStatsValue *table_stats_value,
                                 catalog::CatalogAccessor *accessor);
};
}  // namespace noisepage::optimizer
