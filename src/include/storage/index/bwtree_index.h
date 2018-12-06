#pragma once

#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

// clang-format off
#define IndexCounterMembers(f) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete)
// clang-format on
DEFINE_PERFORMANCE_CLASS(IndexCounter, IndexCounterMembers)
#undef IndexCounterMembers

template <typename KeyType, typename KeyComparator, typename KeyEqualityChecker, typename KeyHashFunc>
class BwTreeIndex final : public Index {
 private:
  using bwtree = third_party::bwtree::BwTree<KeyType, TupleSlot, KeyComparator, KeyEqualityChecker, KeyHashFunc>;

  BwTreeIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type)
      : oid_{oid},
        constraint_type_{constraint_type},
        key_comparator_{},
        key_equality_checker_{},
        key_hash_func_{},
        bwtree_{new bwtree{false, key_comparator_, key_equality_checker_, key_hash_func_}} {}

  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;
  const KeyComparator key_comparator_;
  const KeyEqualityChecker key_equality_checker_;
  const KeyHashFunc key_hash_func_;
  bwtree *const bwtree_;

 public:
  ~BwTreeIndex() final { delete bwtree_; }

  bool Insert(const ProjectedRow &tuple, const TupleSlot location) final {
    TERRIER_ASSERT(constraint_type_ == ConstraintType::DEFAULT,
                   "This Insert is designed for secondary indexes with no primary key or uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromKey(tuple);
    return bwtree_->Insert(index_key, location, false);
  }

  bool Delete(const ProjectedRow &tuple, const TupleSlot location) final {
    KeyType index_key;
    index_key.SetFromKey(tuple);
    return bwtree_->Delete(index_key, location);
  }

  bool ConditionalInsert(const ProjectedRow &tuple, const TupleSlot location,
                         std::function<bool(const void *)> predicate) final {
    TERRIER_ASSERT(constraint_type_ == ConstraintType::PRIMARY_KEY || constraint_type_ == ConstraintType::UNIQUE,
                   "This Insert is designed for indexes with primary key or uniqueness constraints.");
    KeyType index_key;
    index_key.SetFromKey(tuple);

    bool predicate_satisfied = false;

    // This function will complete them in one step predicate will be set to nullptr if the predicate returns true for
    // some value
    const bool ret = bwtree_->ConditionalInsert(index_key, location, predicate, &predicate_satisfied);

    // If predicate is not satisfied then we know insertion succeeds
    if (!predicate_satisfied) {
      TERRIER_ASSERT(ret, "Insertion should always succeed. (Ziqi)");
    } else {
      TERRIER_ASSERT(!ret, "Insertion should always fail. (Ziqi)");
    }

    return ret;
  }
};

}  // namespace terrier::storage::index
