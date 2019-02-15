#pragma once

#include <utility>
#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

class IndexBuilder {
 private:
  catalog::index_oid_t index_oid_;
  ConstraintType constraint_type_ = ConstraintType::INVALID;
  std::vector<catalog::col_oid_t> col_oids_;
  const SqlTable::DataTableVersion *data_table_version_;

 public:
  IndexBuilder() = default;

  Index *Build() const {
    TERRIER_ASSERT(!col_oids_.empty(), "Cannot build an index without col_oids.");
    TERRIER_ASSERT(constraint_type_ != ConstraintType::INVALID, "Cannot build an index without a ConstraintType.");

    bool all_int_attrs = true;
    uint32_t key_size = 0;

    std::vector<uint8_t> attr_sizes;
    std::vector<uint16_t> attr_offsets;

    attr_sizes.reserve(col_oids_.size());
    attr_offsets.reserve(col_oids_.size());

    for (const catalog::col_oid_t col_oid : col_oids_) {
      TERRIER_ASSERT(data_table_version_->column_map.count(col_oid) > 0,
                     "Requested col_oid does not exist in this schema.");
      const col_id_t col_id = data_table_version_->column_map.at(col_oid);
      const uint8_t attr_size = data_table_version_->layout.AttrSize(col_id);
      const type::TypeId attr_type = data_table_version_->schema.GetColumn(col_oid).GetType();

      attr_sizes.emplace_back(attr_size);
      attr_offsets.emplace_back(col_id);
      key_size += attr_size;

      switch (attr_type) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          break;
        default:
          all_int_attrs = false;
          break;
      }
    }

    bool can_use_integer_keys = all_int_attrs && key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS;
    TERRIER_ASSERT(can_use_integer_keys, "Currently, we only have integer keys of limited size.");

    return BuildBwTreeIntsKey(index_oid_, constraint_type_, key_size, std::move(attr_sizes), std::move(attr_offsets));
  }

  IndexBuilder &SetOid(const catalog::index_oid_t index_oid) {
    index_oid_ = index_oid;
    return *this;
  }

  IndexBuilder &SetConstraintType(const ConstraintType constraint_type) {
    constraint_type_ = constraint_type;
    return *this;
  }

  IndexBuilder &SetColOids(const std::vector<catalog::col_oid_t> &col_oids) {
    col_oids_ = col_oids;
    return *this;
  }

  IndexBuilder &SetDataTableVersion(const SqlTable::DataTableVersion *data_table_version) {
    data_table_version_ = data_table_version;
    return *this;
  }

 private:
  Index *BuildBwTreeIntsKey(catalog::index_oid_t index_oid, ConstraintType constraint_type, uint32_t key_size,
                            std::vector<uint8_t> attr_sizes, std::vector<uint16_t> attr_offsets) const {
    TERRIER_ASSERT(key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS, "Not enough slots for given key size.");
    Index *index = nullptr;
    if (key_size <= sizeof(uint64_t)) {
      index = new BwTreeIndex<CompactIntsKey<1>, CompactIntsComparator<1>, CompactIntsEqualityChecker<1>,
                              CompactIntsHasher<1>>(index_oid, constraint_type, attr_sizes, attr_offsets);
    } else if (key_size <= sizeof(uint64_t) * 2) {
      index = new BwTreeIndex<CompactIntsKey<2>, CompactIntsComparator<2>, CompactIntsEqualityChecker<2>,
                              CompactIntsHasher<2>>(index_oid, constraint_type, attr_sizes, attr_offsets);
    } else if (key_size <= sizeof(uint64_t) * 3) {
      index = new BwTreeIndex<CompactIntsKey<3>, CompactIntsComparator<3>, CompactIntsEqualityChecker<3>,
                              CompactIntsHasher<3>>(index_oid, constraint_type, attr_sizes, attr_offsets);
    } else if (key_size <= sizeof(uint64_t) * 4) {
      index = new BwTreeIndex<CompactIntsKey<4>, CompactIntsComparator<4>, CompactIntsEqualityChecker<4>,
                              CompactIntsHasher<4>>(index_oid, constraint_type, attr_sizes, attr_offsets);
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }

};  // class Builder

}  // namespace terrier::storage::index