#pragma once

#include <unordered_map>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"

namespace terrier::storage {
class SqlTable;
namespace index {
class Index;
}
}  // namespace terrier::storage

namespace terrier::catalog {
class CatalogAccessor;
class CatalogCache {
 public:
 private:
  common::ManagedPointer<storage::SqlTable> GetTable(const table_oid_t table) {
    const auto key = static_cast<uint32_t>(table);
    const auto it = pointers_.find(key);
    if (it != pointers_.end()) {
      const auto value = it->second;
      return common::ManagedPointer(reinterpret_cast<storage::SqlTable *>(value));
    }
    return nullptr;
  }

  void PutTable(const table_oid_t table, const common::ManagedPointer<const storage::SqlTable> table_ptr) {
    const auto key = static_cast<uint32_t>(table);
    const auto value = reinterpret_cast<uintptr_t>(table_ptr.Get());
    TERRIER_ASSERT(pointers_.count(key) == 0, "Shouldn't be inserting something that already exists.");
    pointers_[key] = value;
  }

  common::ManagedPointer<storage::index::Index> GetIndex(const index_oid_t index) {
    const auto key = static_cast<uint32_t>(index);
    const auto it = pointers_.find(key);
    if (it != pointers_.end()) {
      const auto value = it->second;
      return common::ManagedPointer(reinterpret_cast<storage::index::Index *>(value));
    }
    return nullptr;
  }

  void PutIndex(const index_oid_t index, const common::ManagedPointer<const storage::index::Index> index_ptr) {
    const auto key = static_cast<uint32_t>(index);
    const auto value = reinterpret_cast<uintptr_t>(index_ptr.Get());
    TERRIER_ASSERT(pointers_.count(key) == 0, "Shouldn't be inserting something that already exists.");
    pointers_[key] = value;
  }

  std::pair<bool, std::vector<index_oid_t>> GetIndexOids(const table_oid_t table) {
    const auto it = indexes_.find(table);
    if (it != indexes_.end()) {
      return {true, it->second};
    }
    return {false, {}};
  }

  void PutIndexOids(const table_oid_t table, std::vector<index_oid_t> &&indexes) {
    TERRIER_ASSERT(indexes_.count(table) == 0, "Shouldn't be inserting something that already exists.");
    indexes_[table] = std::move(indexes);
  }

  void Reset() {
    pointers_.clear();
    indexes_.clear();
  }

  friend class CatalogAccessor;

  std::unordered_map<uint32_t, uintptr_t> pointers_;
  std::unordered_map<table_oid_t, std::vector<index_oid_t>> indexes_;
};

}  // namespace terrier::catalog
