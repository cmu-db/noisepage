#pragma once

#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/index_defs.h"
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

class BwTreeIndex {
 private:
  BwTreeIndex(const catalog::index_oid_t index_oid, const ConstraintType constraint_type)
      : index_oid_{index_oid}, constraint_type_{constraint_type} {}
  const catalog::index_oid_t index_oid_;
  const ConstraintType constraint_type_;

 public:
  class Builder {
   private:
    catalog::index_oid_t index_oid_;
    ConstraintType constraint_type_;

   public:
    catalog::index_oid_t GetOid() const { return index_oid_; }
    void SetOid(const catalog::index_oid_t index_oid) { index_oid_ = index_oid; }
    ConstraintType GetConstraintType() const { return constraint_type_; }
    void SetConstraintType(const ConstraintType constraint_type) { constraint_type_ = constraint_type; }
  };

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return index_oid_; }
};

}  // namespace terrier::storage::index
