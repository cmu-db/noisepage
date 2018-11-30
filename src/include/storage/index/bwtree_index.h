#pragma once

#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

// clang-format off
#define IndexCounterMembers(f) \
  f(uint64_t, NumSelect) \
  f(uint64_t, NumUpdate) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete) \
  f(uint64_t, NumNewBlock)
// clang-format on
DEFINE_PERFORMANCE_CLASS(IndexCounter, IndexCounterMembers)
#undef IndexCounterMembers

template <typename KeyType, typename KeyComparator, typename KeyEqualityChecker, typename KeyHashFunc>
class BwTreeIndex {
 private:
  BwTreeIndex(const catalog::index_oid_t oid, const ConstraintType constraint_type)
      : oid_{oid}, constraint_type_{constraint_type} {}
  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;
  third_party::bwtree::BwTree<KeyType, TupleSlot, KeyComparator, KeyEqualityChecker, KeyHashFunc> *const bwtree_;

  bool Insert(const ProjectedRow &tuple, const TupleSlot location) {
    KeyType index_key;
    index_key.SetFromKey(tuple);
    return bwtree_->Insert(index_key, location, HasUniqueKeys());
  }

  bool Delete(const ProjectedRow &tuple, const TupleSlot location) {
    KeyType index_key;
    index_key.SetFromKey(tuple);
    return bwtree_->Delete(index_key, location);
  }

  bool ConditionalInsert(const ProjectedRow &tuple, const TupleSlot location,
                         std::function<bool(const void *)> predicate) {
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

  bool HasUniqueKeys() const {
    return constraint_type_ == ConstraintType::PRIMARY_KEY || constraint_type_ == ConstraintType::UNIQUE;
  }

 public:
  class Builder {
   private:
    catalog::index_oid_t index_oid_;
    ConstraintType constraint_type_;
    std::vector<catalog::col_oid_t> col_oids_;
    const SqlTable *sql_table_;

   public:
    Builder() = default;

    void Build() const { auto stuffs = sql_table_->InitializerForProjectedRow(col_oids_); }

    Builder &SetOid(const catalog::index_oid_t index_oid) {
      index_oid_ = index_oid;
      return *this;
    }

    Builder &SetConstraintType(const ConstraintType constraint_type) {
      constraint_type_ = constraint_type;
      return *this;
    }

    Builder &SetColOids(const std::vector<catalog::col_oid_t> &col_oids) {
      col_oids_ = col_oids;
      return *this;
    }

    Builder &SetSqlTable(const SqlTable *const sql_table) {
      sql_table_ = sql_table;
      return *this;
    }
  };  // class Builder

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return oid_; }
};

}  // namespace terrier::storage::index
