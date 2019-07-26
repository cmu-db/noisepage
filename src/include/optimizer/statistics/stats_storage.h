#pragma once

#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "common/macros.h"

#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/table_stats.h"

#include <sstream>

namespace terrier::optimizer {

/**
 * An object made to be hashable so it can be a key to the table stats
 * storage map.
 */
class StatsStorageKey {
 public:
  StatsStorageKey(catalog::db_oid_t database_id, catalog::table_oid_t table_id)
      : database_id_(database_id),
        table_id_(table_id) {}

  /**
   * Defined hash function for StatsStorageKey object.
   * @return the hash for the object
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(database_id_);
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_id_));
    return hash;
  }

  /**
   *
   * @return
   */
  catalog::db_oid_t GetDatabaseID() const { return database_id_; }

  /**
   *
   * @return
   */
  catalog::table_oid_t GetTableID() const { return table_id_; }

 private:
  /**
   * oid of database
   */
  catalog::db_oid_t database_id_;

  /**
   * oid of table
   */
  catalog::table_oid_t table_id_;
};
}

namespace std {
/**
 * template for std::hash of StatsStorageKey
 */
template <>
struct hash<terrier::optimizer::StatsStorageKey> {
  /**
   * Hashes a StatsStorageKey object.
   * @param stats_storage_key - StatsStorageKey object
   * @return the hash for the StatsStorageKey
   */
  size_t operator()(const terrier::optimizer::StatsStorageKey &stats_storage_key) const
  { return stats_storage_key.Hash(); }
};

/**
 *
 */
template <>
struct equal_to<terrier::optimizer::StatsStorageKey> {
  bool operator()(const terrier::optimizer::StatsStorageKey &lhs,
                  const terrier::optimizer::StatsStorageKey &rhs) const {
    return lhs.GetDatabaseID() == rhs.GetDatabaseID() && lhs.GetTableID() == rhs.GetTableID();
  }
};
}

namespace terrier::optimizer {

class StatsStorage {
 public:
  /**
   * GetPtrToTableStats - Using given database and table ids,
   * select a pointer to the TableStats objects in the table stats storage map.
   * @param database_id - oid of database
   * @param table_id - oid of table
   * @return pointer to a TableStats object
   */
  TableStats *GetPtrToTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id);

  /**
   * GetPtrToTableStatsStorage - gets the pointer to the unordered map storing
   * all the table stats objects.
   * @return a pointer to the table stats storage map
   */
  std::unordered_map<StatsStorageKey, TableStats *> *GetPtrToTableStatsStorage() {
    return &table_stats_storage;
  }

 protected:
  /**
   * InsertOrUpdateTableStats - if there is no corresponding pointer to a TableStats object
   * for the given database and table ids in the stats storage map, then this function inserts
   * a TableStats pointer in the table stats storage map. Else, it removes the TableStats pointer
   * currently stored and inserts the TableStats pointer given in its place.
   * @param database_id
   * @param table_id
   * @param table_stats
   */
  void InsertOrUpdateTableStats(catalog::db_oid_t database_id,
                                catalog::table_oid_t table_id, TableStats *table_stats);

  /**
   * DeleteTableStats - if there is a corresponding pointer to a TableStats object
   * @param database_id
   * @param table_id
   */
  void DeleteTableStats(catalog::db_oid_t database_id,
                        catalog::table_oid_t table_id);

 private:
  /**
   * An unordered map mapping StatsStorageKey objects (database_id and table_id) to
   * TableStats pointers. This represents the storage for TableStats objects.
   */
  std::unordered_map<StatsStorageKey, TableStats *> table_stats_storage;
};
}  // namespace terrier::optimizer


