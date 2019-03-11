#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/performance_counter.h"
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

class Index {
 private:
  const catalog::index_oid_t oid_;
  const ConstraintType constraint_type_;
  const IndexMetadata metadata_;

 protected:
  Index(const catalog::index_oid_t oid, const ConstraintType constraint_type, IndexMetadata metadata)
      : oid_{oid}, constraint_type_{constraint_type}, metadata_(std::move(metadata)) {}

 public:
  virtual ~Index() = default;

  virtual bool Insert(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool Delete(const ProjectedRow &tuple, TupleSlot location) = 0;

  virtual bool ConditionalInsert(const ProjectedRow &tuple, TupleSlot location,
                                 std::function<bool(const TupleSlot)> predicate) = 0;

  virtual void ScanKey(const ProjectedRow &key, std::vector<TupleSlot> *value_list) = 0;

  ConstraintType GetConstraintType() const { return constraint_type_; }
  catalog::index_oid_t GetOid() const { return oid_; }

  const std::vector<KeyData> &GetKeySchema() const { return metadata_.GetKeySchema(); }
  const std::vector<uint16_t> &GetProjectedRowOffsets() const { return metadata_.GetProjectedRowOffsets(); }
  const std::vector<uint16_t> &GetComparisonOrder() const { return metadata_.GetComparisonOrder(); }
  const std::vector<uint8_t> &GetAttributeSizes() const { return metadata_.GetAttributeSizes(); }
  const std::vector<uint8_t> &GetAttributeOffsets() const { return metadata_.GetAttributeOffsets(); }
  const uint32_t GetOffset(key_oid_t key_oid) const { return metadata_.GetKeyOidToOffsetMap().at(key_oid); }
};

}  // namespace terrier::storage::index
