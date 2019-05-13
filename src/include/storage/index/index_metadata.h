#pragma once

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "storage/index/index_defs.h"
#include "storage/projected_row.h"
#include "storage/storage_util.h"
#include "type/type_util.h"

namespace terrier::storage::index {

/**
 * Precomputes index-related metadata that can be used to optimize the operations of the various index key types.
 */
class IndexMetadata {
 public:
  /**
   * Move-construct the index metadata object.
   * @param other move source
   */
  IndexMetadata(IndexMetadata &&other) noexcept
      : key_schema_(std::move(other.key_schema_)),
        attr_sizes_(std::move(other.attr_sizes_)),
        inlined_attr_sizes_(std::move(other.inlined_attr_sizes_)),
        must_inline_varlen_(other.must_inline_varlen_),
        compact_ints_offsets_(std::move(other.compact_ints_offsets_)),
        key_oid_to_offset_(std::move(other.key_oid_to_offset_)),
        initializer_(std::move(other.initializer_)),
        inlined_initializer_(std::move(other.inlined_initializer_)) {}

  /**
   * Precomputes metadata for the given key schema.
   * @param key_schema index key schema
   */
  explicit IndexMetadata(IndexKeySchema key_schema)
      : key_schema_(std::move(key_schema)),
        attr_sizes_(ComputeAttributeSizes(key_schema_)),
        inlined_attr_sizes_(ComputeInlinedAttributeSizes(key_schema_)),
        must_inline_varlen_(ComputeMustInlineVarlen(key_schema_)),
        compact_ints_offsets_(ComputeCompactIntsOffsets(attr_sizes_)),
        key_oid_to_offset_(ComputeKeyOidToOffset(key_schema_, ComputePROffsets(inlined_attr_sizes_))),
        initializer_(
            ProjectedRowInitializer::Create(GetRealAttrSizes(attr_sizes_), ComputePROffsets(inlined_attr_sizes_))),
        inlined_initializer_(
            ProjectedRowInitializer::Create(inlined_attr_sizes_, ComputePROffsets(inlined_attr_sizes_))) {}

  /**
   * @return index key schema
   */
  const std::vector<IndexKeyColumn> &GetKeySchema() const { return key_schema_; }

  /**
   * @return unsorted index attribute sizes (key schema order), varlens are marked
   */
  const std::vector<uint8_t> &GetAttributeSizes() const { return attr_sizes_; }

  /**
   * @return actual inlined index attribute sizes
   */
  const std::vector<uint16_t> &GetInlinedAttributeSizes() const { return inlined_attr_sizes_; }

  /**
   * @return true if the varlens must be inlined, i.e. we cannot rely on VarlenEntry inlining as varlens are too big
   */
  bool MustInlineVarlen() const { return must_inline_varlen_; }

  /**
   * @return offsets to write into for compact ints (key schema order)
   */
  const std::vector<uint8_t> &GetCompactIntsOffsets() const { return compact_ints_offsets_; }

  /**
   * @return mapping from key oid to projected row offset
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &GetKeyOidToOffsetMap() const {
    return key_oid_to_offset_;
  }

  /**
   * @return projected row initializer for the given key schema
   */
  const ProjectedRowInitializer &GetProjectedRowInitializer() const { return initializer_; }

  /**
   * @return projected row initializer for the inlined version of the key schema
   */
  const ProjectedRowInitializer &GetInlinedPRInitializer() const { return inlined_initializer_; }

 private:
  DISALLOW_COPY(IndexMetadata);
  FRIEND_TEST(BwTreeKeyTests, IndexMetadataCompactIntsKeyTest);
  FRIEND_TEST(BwTreeKeyTests, IndexMetadataGenericKeyNoMustInlineVarlenTest);
  FRIEND_TEST(BwTreeKeyTests, IndexMetadataGenericKeyMustInlineVarlenTest);

  std::vector<IndexKeyColumn> key_schema_;                                      // for GenericKey
  std::vector<uint8_t> attr_sizes_;                                             // for CompactIntsKey
  std::vector<uint16_t> inlined_attr_sizes_;                                    // for GenericKey
  bool must_inline_varlen_;                                                     // for GenericKey
  std::vector<uint8_t> compact_ints_offsets_;                                   // for CompactIntsKey
  std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> key_oid_to_offset_;  // for execution layer
  ProjectedRowInitializer initializer_;                                         // user-facing initializer
  ProjectedRowInitializer inlined_initializer_;                                 // for GenericKey, internal only

  /**
   * Computes the attribute sizes as given by the key schema.
   * e.g.   if key_schema is {INTEGER, INTEGER, BIGINT, TINYINT, SMALLINT}
   *        then attr_sizes returned is {4, 4, 8, 1, 2}
   */
  static std::vector<uint8_t> ComputeAttributeSizes(const IndexKeySchema &key_schema) {
    std::vector<uint8_t> attr_sizes;
    attr_sizes.reserve(key_schema.size());
    for (const auto &key : key_schema) {
      attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(key.GetType()));
    }
    return attr_sizes;
  }

  /**
   * Computes the attribute sizes as given by the key schema if everything were inlined.
   * Note varchars are inlined as VarlenEntry if they fit, and as (4 bytes of size + varlen content) otherwise.
   * e.g.   if key_schema is {INTEGER, VARCHAR(8), VARCHAR(0), TINYINT, VARCHAR(12)}
   *        then attr_sizes returned is {4, 16, 16, 1, 16}
   */
  static std::vector<uint16_t> ComputeInlinedAttributeSizes(const IndexKeySchema &key_schema) {
    std::vector<uint16_t> inlined_attr_sizes;
    inlined_attr_sizes.reserve(key_schema.size());
    for (const auto &key : key_schema) {
      auto key_type = key.GetType();
      switch (key_type) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR: {
          // Add 4 bytes because we'll prepend a size field. If we're too small, we'll just use a VarlenEntry.
          auto varlen_size =
              std::max(static_cast<uint16_t>(key.GetMaxVarlenSize() + 4), static_cast<uint16_t>(sizeof(VarlenEntry)));
          inlined_attr_sizes.emplace_back(varlen_size);
          break;
        }
        default:
          inlined_attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(key_type));
          break;
      }
    }
    return inlined_attr_sizes;
  }

  /**
   * Computes whether we need to manually inline varlen attributes, i.e. too big for VarlenEntry::CreateInline.
   */
  static bool ComputeMustInlineVarlen(const IndexKeySchema &key_schema) {
    return std::any_of(key_schema.begin(), key_schema.end(), [](const auto &key) -> bool {
      switch (key.GetType()) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR:
          return key.GetMaxVarlenSize() > VarlenEntry::InlineThreshold();
        default:
          break;
      }
      return false;
    });
  }

  /**
   * Computes the compact int offsets for the given vector of attribute sizes.
   * e.g.   if attr_sizes {4, 4, 8, 1, 2}
   *        exclusive scan {0, 4, 8, 16, 17}
   *        since offset[i] = where to write sorted attr i in a compact ints key
   */
  static std::vector<uint8_t> ComputeCompactIntsOffsets(const std::vector<uint8_t> &attr_sizes) {
    // exclusive scan
    std::vector<uint8_t> scan;
    scan.reserve(attr_sizes.size());
    scan.emplace_back(0);
    for (uint16_t i = 1; i < attr_sizes.size(); i++) {
      scan.emplace_back(scan[i - 1] + attr_sizes[i - 1]);
    }
    return scan;
  }

  /**
   * Computes the projected row offsets given the attribute sizes.
   * e.g.   if attr_sizes is {4, 4, 8, 1, 2}
   *        then returned offsets are {1, 2, 0, 4, 3}
   *        since offset[i] = where attr i ended up after sorting
   */
  static std::vector<uint16_t> ComputePROffsets(const std::vector<uint16_t> &attr_sizes) {
    // tuple of size and original index
    std::vector<std::pair<uint16_t, uint16_t>> size_idx;
    size_idx.reserve(attr_sizes.size());
    for (uint16_t i = 0; i < attr_sizes.size(); i++) {
      size_idx.emplace_back(std::make_pair(attr_sizes[i], i));
    }
    // sort by the sizes
    std::stable_sort(size_idx.begin(), size_idx.end(),
                     [](const auto &u, const auto &v) -> bool { return u.first > v.first; });
    // read off the pr_offsets
    std::vector<uint16_t> pr_offsets(attr_sizes.size());
    for (uint16_t i = 0; i < size_idx.size(); i++) {
      // rank[original index] = position after sort
      pr_offsets[size_idx[i].second] = i;
    }
    return pr_offsets;
  }

  /**
   * Computes the mapping from key oid to projected row offset.
   */
  static std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> ComputeKeyOidToOffset(
      const IndexKeySchema &key_schema, const std::vector<uint16_t> &pr_offsets) {
    std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> key_oid_to_offset;
    key_oid_to_offset.reserve(key_schema.size());
    for (uint16_t i = 0; i < key_schema.size(); i++) {
      key_oid_to_offset[key_schema[i].GetOid()] = pr_offsets[i];
    }
    return key_oid_to_offset;
  }

  /**
   * By default, the uint8_t attr_sizes that we pass around in our system are not the real attribute sizes.
   * The MSB is set to indicate whether a column is VARLEN or otherwise. We mask these off to get the real sizes.
   */
  static std::vector<uint8_t> GetRealAttrSizes(std::vector<uint8_t> attr_sizes) {
    std::transform(attr_sizes.begin(), attr_sizes.end(), attr_sizes.begin(),
                   [](uint8_t elem) -> uint8_t { return static_cast<uint8_t>(elem & INT8_MAX); });
    return attr_sizes;
  }
};

}  // namespace terrier::storage::index
