#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
class SqlTable;
namespace index {
class Index;
}
}  // namespace noisepage::storage

namespace noisepage::catalog {
class CatalogAccessor;

/**
 * Simple cache for DatabaseCatalog lookups that's scoped per connection (ownership and lifecycle). This is designed to
 * be injected as a dependency of CatalogAccessor at its instantiation, and components requesting information from the
 * CatalogAccessor will transparently look in a cache first if it exists. If the cache is passed in as nullptr, then the
 * CatalogAccessor performs its lookup from the DatabaseCatalog as normal. Most operations are expected to only be
 * performed by CatalogAccessor, which is why most of this class is private and the CatalogAccessor is designated as a
 * friend class.
 *
 * Right now it only caches lookups for table and index pointers, and indexes on a table. In the future if we see other
 * DatabaseCatalog requests proving expensive, we can add them to the cache.
 */
class CatalogCache {
 public:
  /**
   * Clear the cache, and stash a timestamp associated with the start time of the transaction that cleared it. This acts
   * as a watermark for when this snapshot of the DatabaseCatalog is valid.
   * @param now start time of the TransactionContext performing the reset
   */
  void Reset(const transaction::timestamp_t now) {
    oldest_entry_ = now;
    pointers_.clear();
    indexes_.clear();
  }

  /**
   * @return The oldest possible age of an entry in this cache, corresponding to the last cache reset
   */
  transaction::timestamp_t OldestEntry() const { return oldest_entry_; }

 private:
  common::ManagedPointer<storage::SqlTable> GetTable(const table_oid_t table) {
    const auto key = table.UnderlyingValue();
    const auto it = pointers_.find(key);
    if (it != pointers_.end()) {
      const auto value = it->second;
      return common::ManagedPointer(reinterpret_cast<storage::SqlTable *>(value));
    }
    return nullptr;
  }

  void PutTable(const table_oid_t table, const common::ManagedPointer<storage::SqlTable> table_ptr) {
    const auto key = table.UnderlyingValue();
    const auto value = reinterpret_cast<uintptr_t>(table_ptr.Get());
    NOISEPAGE_ASSERT(pointers_.count(key) == 0, "Shouldn't be inserting something that already exists.");
    pointers_[key] = value;
  }

  common::ManagedPointer<storage::index::Index> GetIndex(const index_oid_t index) {
    const auto key = index.UnderlyingValue();
    const auto it = pointers_.find(key);
    if (it != pointers_.end()) {
      const auto value = it->second;
      return common::ManagedPointer(reinterpret_cast<storage::index::Index *>(value));
    }
    return nullptr;
  }

  void PutIndex(const index_oid_t index, const common::ManagedPointer<storage::index::Index> index_ptr) {
    const auto key = index.UnderlyingValue();
    const auto value = reinterpret_cast<uintptr_t>(index_ptr.Get());
    NOISEPAGE_ASSERT(pointers_.count(key) == 0, "Shouldn't be inserting something that already exists.");
    pointers_[key] = value;
  }

  std::pair<bool, std::vector<index_oid_t>> GetIndexOids(const table_oid_t table) {
    const auto it = indexes_.find(table);
    if (it != indexes_.end()) {
      // return true to indicate table was found, but list could still be empty
      return {true, it->second};
    }
    // return false to indidcate table was not found
    return {false, {}};
  }

  void PutIndexOids(const table_oid_t table, std::vector<index_oid_t> indexes) {
    NOISEPAGE_ASSERT(indexes_.count(table) == 0, "Shouldn't be inserting something that already exists.");
    indexes_[table] = std::move(indexes);
  }

  friend class CatalogAccessor;

  std::unordered_map<uint32_t, uintptr_t> pointers_;
  std::unordered_map<table_oid_t, std::vector<index_oid_t>> indexes_;
  transaction::timestamp_t oldest_entry_ = transaction::INITIAL_TXN_TIMESTAMP;
};

}  // namespace noisepage::catalog
