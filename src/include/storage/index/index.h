#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
#include "storage/index/index_defs.h"
#include "storage/storage_defs.h"

// clang-format off
#define IndexCounterMembers(f) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete)
// clang-format on
DEFINE_PERFORMANCE_CLASS(IndexCounter, IndexCounterMembers)
#undef IndexCounterMembers

namespace terrier::storage::index {

class Index {
 private:
  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;
  const std::vector<uint8_t> attr_sizes_;
  const std::vector<uint16_t> attr_offsets_;

 protected:
  Index(const catalog::index_oid_t oid, const ConstraintType constraint_type, std::vector<uint8_t> attr_sizes,
        std::vector<uint16_t> attr_offsets)
      : oid_{oid},
        constraint_type_{constraint_type},
        attr_sizes_(std::move(attr_sizes)),
        attr_offsets_(std::move(attr_offsets)) {}

 public:
  virtual ~Index() = default;

  virtual bool Insert(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool Delete(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool ConditionalInsert(const ProjectedRow &tuple, TupleSlot location,
                                 std::function<bool(const TupleSlot)> predicate) = 0;

  virtual void ScanKey(const ProjectedRow &key, std::vector<TupleSlot> *value_list) = 0;

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return oid_; }

  std::vector<uint8_t> GetAttrSizes() const { return attr_sizes_; }

  std::vector<uint16_t> GetAttrOffsets() const { return attr_offsets_; }
};

}  // namespace terrier::storage::index
