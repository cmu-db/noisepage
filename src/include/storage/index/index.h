#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/storage_defs.h"

// clang-format off
#define IndexCounterMembers(f) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete)
// clang-format on
DEFINE_PERFORMANCE_CLASS(IndexCounter, IndexCounterMembers)
#undef IndexCounterMembers

namespace terrier::storage::index {

/**
 * Wrapper class for the various types of indexes in our system.
 */
class Index {
 private:
  // make friends with our keys so that they can see our metadata
  friend class CompactIntsKey<1>;
  friend class CompactIntsKey<2>;
  friend class CompactIntsKey<3>;
  friend class CompactIntsKey<4>;
  friend class GenericKey<64>;
  friend class GenericKey<128>;
  friend class GenericKey<256>;

  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;

 protected:
  const IndexMetadata metadata_;  // cached metadata

  Index(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : oid_{oid}, constraint_type_{constraint_type}, metadata_(std::move(metadata)) {}

 public:
  virtual ~Index() = default;

  virtual bool Insert(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool Delete(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool ConditionalInsert(const ProjectedRow &tuple, TupleSlot location,
                                 std::function<bool(const TupleSlot)> predicate) = 0;

  /**
   * Looks for all the values associated with the given key in our index.
   * @param key the key to look for
   * @param[out] value_list the values associated with the key
   */
  virtual void ScanKey(const ProjectedRow &key, std::vector<TupleSlot> *value_list) = 0;

  /**
   * @return type of this index
   */
  ConstraintType GetConstraintType() const { return constraint_type_; }

  /**
   * @return oid of this indes
   */
  catalog::index_oid_t GetOid() const { return oid_; }
};

}  // namespace terrier::storage::index
