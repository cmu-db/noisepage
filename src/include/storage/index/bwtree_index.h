#pragma once

#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

template <typename KeyType, typename KeyComparator, typename KeyEqualityChecker, typename KeyHashFunc>
class BwTreeIndex final : public Index {
  friend class IndexBuilder;

 private:
  BwTreeIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type, const IndexMetadata &metadata)
      : Index(oid, constraint_type, metadata),
        bwtree_{new third_party::bwtree::BwTree<KeyType, TupleSlot, KeyComparator, KeyEqualityChecker, KeyHashFunc>{
            false, KeyComparator{}, KeyEqualityChecker{}, KeyHashFunc{}}} {}

  third_party::bwtree::BwTree<KeyType, TupleSlot, KeyComparator, KeyEqualityChecker, KeyHashFunc> *const bwtree_;

 public:
  ~BwTreeIndex() final { delete bwtree_; }

  bool Insert(const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no primary key or uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, GetAttributeSizes(), GetAttributeOffsets());
    auto unique_keys = GetConstraintType() == ConstraintType::UNIQUE;
    return bwtree_->Insert(index_key, location, unique_keys);
  }

  bool Delete(const ProjectedRow &tuple, const TupleSlot location) final {
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, GetAttributeSizes(), GetAttributeOffsets());
    return bwtree_->Delete(index_key, location);
  }

  bool ConditionalInsert(const ProjectedRow &tuple, const TupleSlot location,
                         std::function<bool(const TupleSlot)> predicate) final {
    TERRIER_ASSERT(GetConstraintType() == ConstraintType::PRIMARY_KEY || GetConstraintType() == ConstraintType::UNIQUE,
                   "This Insert is designed for indexes with primary key or uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromProjectedRow(tuple, GetAttributeSizes(), GetAttributeOffsets());
    bool predicate_satisfied = false;

    // predicate is set to nullptr if the predicate returns true for some value
    const bool ret = bwtree_->ConditionalInsert(index_key, location, predicate, &predicate_satisfied);

    // if predicate is not satisfied then we know insertion succeeds
    if (!predicate_satisfied) {
      TERRIER_ASSERT(ret, "Insertion should always succeed. (Ziqi)");
    } else {
      TERRIER_ASSERT(!ret, "Insertion should always fail. (Ziqi)");
    }

    return ret;
  }

  void ScanKey(const ProjectedRow &key, std::vector<TupleSlot> *value_list) final {
    KeyType index_key;
    index_key.SetFromProjectedRow(key, GetAttributeSizes(), GetAttributeOffsets());
    bwtree_->GetValue(index_key, *value_list);
  }
};

}  // namespace terrier::storage::index
